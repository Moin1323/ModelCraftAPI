import Vapor
@preconcurrency import RealityKit
import UniformTypeIdentifiers
import Foundation
import CoreGraphics
import ImageIO
import ZIPFoundation
import NIO

// Progress Response Struct
struct ProgressResponse: Content {
    let statusMessage: String
    let progress: Double
    let processingStage: String
    let isProcessing: Bool
}

// Storage Keys
private struct ProcessingStateKey: StorageKey {
    typealias Value = ProcessingState
}

private struct ActiveTasksKey: StorageKey {
    typealias Value = Int
}

// Request-Scoped State
private struct ProcessingState {
    var selectedFolderURL: URL?
    var statusMessage: String = "Processing started"
    var isProcessing: Bool = false
    var progress: Double = 0.0
    var photogrammetrySession: PhotogrammetrySession?
    var processingStage: String = ""
}

@MainActor
final class ModelProcessingController {
    // MARK: - Constants
    private let optimalDetailLevel: PhotogrammetrySession.Request.Detail = .raw
    private let minPoseConfidence: Double = 0.3
    private let minFeatureCount: Int = 10
    private let maxFileSize: Int = 100_000_000 // 100MB limit
    private let maxConcurrentTasks: Int = 5 // Rate limit for concurrent processing

    // MARK: - Core Processing Function
    func processModel(_ req: Request) async throws -> Response {
        // Rate limiting
        let activeTasks = req.application.storage[ActiveTasksKey.self] ?? 0
        guard activeTasks < maxConcurrentTasks else {
            req.logger.warning("Rate limit exceeded: \(activeTasks) active tasks")
            throw Abort(.tooManyRequests, reason: "Server is busy, please try again later")
        }
        req.application.storage[ActiveTasksKey.self] = activeTasks + 1
        defer {
            req.application.storage[ActiveTasksKey.self] = max(0, activeTasks - 1)
        }

        let fileManager = FileManager.default

        // File handling with multiple fallbacks
        let file: File
        do {
            if let formFile = try? req.content.get(FileUpload.self, at: "file") {
                file = File(data: formFile.data, filename: formFile.filename)
                req.logger.info("Received file via 'file' field: \(formFile.filename), size: \(formFile.data.readableBytes) bytes")
            } else if let formFile = try? req.content.get(FileUpload.self, at: "data") {
                file = File(data: formFile.data, filename: formFile.filename)
                req.logger.info("Received file via 'data' field: \(formFile.filename), size: \(formFile.data.readableBytes) bytes")
            } else if let uploadedFile = try? req.content.decode(File.self) {
                file = uploadedFile
                req.logger.info("Received direct file upload: \(uploadedFile.filename), size: \(uploadedFile.data.readableBytes) bytes")
            } else if let bodyData = req.body.data {
                let filename = req.headers.contentDisposition?.filename ??
                              req.headers.first(name: "filename") ??
                              "uploaded_file.zip"
                file = File(data: bodyData, filename: filename)
                req.logger.info("Received file from raw body: \(filename), size: \(bodyData.readableBytes) bytes")
            } else {
                req.logger.error("No file provided in request")
                throw Abort(.badRequest, reason: "No file uploaded or invalid field name")
            }
        } catch {
            req.logger.error("Failed to process file upload: \(error)")
            throw Abort(.badRequest, reason: "Failed to process file upload: \(error)")
        }

        // Validate file size
        guard file.data.readableBytes < maxFileSize else {
            req.logger.error("File size exceeds limit: \(file.data.readableBytes) bytes > \(maxFileSize) bytes")
            throw Abort(.payloadTooLarge, reason: "Uploaded file exceeds 100MB limit")
        }

        let tempDir = fileManager.temporaryDirectory.appendingPathComponent("model_processing_\(UUID().uuidString)")
        let inputDir = tempDir.appendingPathComponent("input")
        let outputDir = tempDir.appendingPathComponent("output")

        do {
            // Create directories
            try fileManager.createDirectory(at: inputDir, withIntermediateDirectories: true)
            try fileManager.createDirectory(at: outputDir, withIntermediateDirectories: true)
            req.logger.info("Created directories: \(inputDir.path), \(outputDir.path)")

            // Ensure filename has .zip extension
            let sanitizedFilename = file.filename.lowercased().hasSuffix(".zip") ?
                file.filename :
                "\(file.filename).zip"
            let zipURL = inputDir.appendingPathComponent(sanitizedFilename)

            // Write file
            req.logger.info("[DEBUG] Starting to write file to \(zipURL.path)")
            let writeStart = Date()
            try await req.fileio.writeFile(file.data, at: zipURL.path)
            let writeDuration = Date().timeIntervalSince(writeStart)
            req.logger.info("[DEBUG] Wrote file to \(zipURL.path), size: \(file.data.readableBytes) bytes, duration: \(writeDuration)s")

            // Verify file
            guard fileManager.fileExists(atPath: zipURL.path) else {
                req.logger.error("ZIP file not found at \(zipURL.path)")
                throw Abort(.internalServerError, reason: "Failed to save uploaded file")
            }

            // Validate ZIP integrity
            req.logger.info("[DEBUG] Verifying ZIP integrity at \(zipURL.path)")
            let zipData = try Data(contentsOf: zipURL)
            guard zipData.count > 0 else {
                req.logger.error("ZIP file is empty at \(zipURL.path)")
                throw Abort(.badRequest, reason: "Uploaded ZIP file is empty")
            }
            req.logger.info("ZIP file size verified: \(zipData.count) bytes")

            // Recursive ZIP extraction
            func extractZip(_ zipURL: URL, to destination: URL, logger: Logger, depth: Int = 0) throws {
                guard depth < 5 else {
                    logger.error("Maximum ZIP extraction depth reached at \(zipURL.path)")
                    throw Abort(.badRequest, reason: "Too many nested ZIP files")
                }
                do {
                    // Log ZIP contents before extraction
                    let archive = try Archive(url: zipURL, accessMode: .read)
                    let entries = archive.map { $0.path }
                    logger.info("[DEBUG] ZIP contents at \(zipURL.path) (depth \(depth)): \(entries)")

                    logger.info("[DEBUG] Starting extraction of \(zipURL.path) to \(destination.path) at depth \(depth)")
                    let extractStart = Date()
                    try fileManager.unzipItem(at: zipURL, to: destination)
                    let extractDuration = Date().timeIntervalSince(extractStart)
                    let contents = try fileManager.contentsOfDirectory(at: destination, includingPropertiesForKeys: [.isDirectoryKey])
                    logger.info("[DEBUG] Extracted contents to \(destination.path) (depth \(depth)): \(contents.map { $0.lastPathComponent }), duration: \(extractDuration)s")
                    
                    // Handle nested ZIPs
                    for item in contents where item.pathExtension.lowercased() == "zip" {
                        logger.info("Found nested ZIP: \(item.lastPathComponent) at depth \(depth)")
                        let nestedDest = destination.appendingPathComponent(item.deletingPathExtension().lastPathComponent)
                        try fileManager.createDirectory(at: nestedDest, withIntermediateDirectories: true)
                        // Save nested ZIP for debugging
                        let debugZipURL = destination.appendingPathComponent("nested_zip_depth_\(depth+1)_\(item.lastPathComponent)")
                        try fileManager.copyItem(at: item, to: debugZipURL)
                        logger.info("[DEBUG] Saved nested ZIP for debugging at \(debugZipURL.path)")
                        try extractZip(item, to: nestedDest, logger: logger, depth: depth + 1)
                        try fileManager.removeItem(at: item)
                        logger.info("Removed nested ZIP: \(item.lastPathComponent)")
                    }
                    
                    // Log subdirectories
                    let subdirs = contents.filter { (item: URL) -> Bool in
                        do {
                            let resourceValues = try item.resourceValues(forKeys: [.isDirectoryKey])
                            return resourceValues.isDirectory == true
                        } catch {
                            logger.warning("Failed to get resource values for \(item.lastPathComponent): \(error)")
                            return false
                        }
                    }
                    logger.info("Subdirectories in \(destination.lastPathComponent): \(subdirs.map { $0.lastPathComponent })")
                } catch {
                    logger.error("Failed to extract ZIP at \(zipURL.path) (depth \(depth)): \(error)")
                    throw Abort(.badRequest, reason: "Failed to extract ZIP file: \(error.localizedDescription)")
                }
            }

            req.logger.info("[DEBUG] Starting ZIP extraction for \(zipURL.path)")
            try extractZip(zipURL, to: inputDir, logger: req.logger)
            
            // Log final extracted structure
            let finalContents = try fileManager.contentsOfDirectory(at: inputDir, includingPropertiesForKeys: [.isDirectoryKey])
            req.logger.info("Final extracted ZIP contents: \(finalContents.map { $0.lastPathComponent })")
            
            // Log contents of subdirectories
            for item in finalContents where try item.resourceValues(forKeys: [.isDirectoryKey]).isDirectory == true {
                let subContents = try fileManager.contentsOfDirectory(at: item, includingPropertiesForKeys: nil)
                req.logger.info("Contents of \(item.lastPathComponent): \(subContents.map { $0.lastPathComponent })")
            }

            // Validate extracted structure
            guard finalContents.contains(where: { $0.lastPathComponent == "images" && (try? $0.resourceValues(forKeys: [.isDirectoryKey]).isDirectory) == true }) else {
                req.logger.error("No 'images' folder found in extracted contents")
                throw Abort(.badRequest, reason: "ZIP file does not contain an 'images' folder")
            }

            // Store state
            var state = ProcessingState(selectedFolderURL: inputDir)
            await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)

            let outputFileURL = outputDir.appendingPathComponent("output_model.usdz")
            req.logger.info("[DEBUG] Starting processing with output URL: \(outputFileURL.path)")
            let processingStart = Date()
            state = try await startProcessing(outputURL: outputFileURL, req: req, state: state)
            let processingDuration = Date().timeIntervalSince(processingStart)
            req.logger.info("[DEBUG] Processing completed, duration: \(processingDuration)s")

            guard fileManager.fileExists(atPath: outputFileURL.path) else {
                req.logger.error("Output model not found at \(outputFileURL.path)")
                throw Abort(.internalServerError, reason: "Failed to generate output model")
            }

            req.logger.info("[DEBUG] Output model found at \(outputFileURL.path), preparing response")
            let response = Response(status: .ok)
            response.headers.contentType = HTTPMediaType(type: "model", subType: "usdz")
            response.headers.add(name: "Content-Disposition", value: "attachment; filename=output_model.usdz")

            return try await req.fileio.asyncStreamFile(
                at: outputFileURL.path,
                chunkSize: 8192,
                mediaType: HTTPMediaType(type: "model", subType: "usdz"),
                advancedETagComparison: false
            )
        } catch {
            req.logger.error("Processing failed: \(error)")
            try? fileManager.removeItem(at: tempDir)
            cleanupTemporaryFolders(req)
            throw Abort(.internalServerError, reason: "Processing failed: \(error.localizedDescription)")
        }
    }

    // Add this struct to handle multipart file uploads
    struct FileUpload: Content {
        var data: ByteBuffer
        var filename: String
    }

    func getProgress(_ req: Request) async throws -> ProgressResponse {
        guard let state = req.storage[ProcessingStateKey.self] else {
            throw Abort(.notFound, reason: "No active processing session")
        }

        req.logger.info("[DEBUG] Progress requested: status=\(state.statusMessage), progress=\(state.progress), stage=\(state.processingStage)")
        return ProgressResponse(
            statusMessage: state.statusMessage,
            progress: state.progress,
            processingStage: state.processingStage,
            isProcessing: state.isProcessing
        )
    }

    // MARK: - Core Processing Function
    private func startProcessing(outputURL: URL, req: Request, state: ProcessingState) async throws -> ProcessingState {
        var state = state
        guard let folderURL = state.selectedFolderURL else {
            state.statusMessage = "⚠️ Please select a folder first"
            await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
            throw Abort(.badRequest, reason: "No folder selected")
        }

        state.isProcessing = true
        state.progress = 0.0
        state.statusMessage = "⏳ Starting reconstruction..."
        state.processingStage = "Preparing input data..."
        req.logger.info("[DEBUG] Starting photogrammetry processing for folder: \(folderURL.path)")
        await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)

        req.logger.info("[DEBUG] Checking if PLY reference should be used")
        let usePLYAsReference = try await shouldUsePLYReference(from: folderURL, req: req, state: &state)
        req.logger.info("[DEBUG] PLY reference decision: \(usePLYAsReference)")

        req.logger.info("[DEBUG] Preparing input folder")
        let prepareStart = Date()
        let (inputFolder, imagesFolder) = try prepareInputFolder(folderURL, usePLY: usePLYAsReference, req: req)
        let prepareDuration = Date().timeIntervalSince(prepareStart)
        req.logger.info("[DEBUG] Input folder prepared: input=\(inputFolder.path), images=\(imagesFolder.path), duration: \(prepareDuration)s")

        var configuration = PhotogrammetrySession.Configuration()
        configuration.sampleOrdering = .sequential
        configuration.featureSensitivity = usePLYAsReference ? .high : .normal
        req.logger.info("[DEBUG] Photogrammetry session configuration: sampleOrdering=\(configuration.sampleOrdering), featureSensitivity=\(configuration.featureSensitivity)")

        req.logger.info("[DEBUG] Initializing photogrammetry session with input: \(imagesFolder.path)")
        let sessionStart = Date()
        let session = try PhotogrammetrySession(input: imagesFolder, configuration: configuration)
        let sessionInitDuration = Date().timeIntervalSince(sessionStart)
        req.logger.info("[DEBUG] Photogrammetry session initialized, duration: \(sessionInitDuration)s")
        state.photogrammetrySession = session
        await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)

        // Create a timeout task to detect hangs
        let timeoutSeconds = 300.0 // 5 minutes, closer to macOS app's 4-minute benchmark
        let timeoutState = state
        let timeoutTask = Task {
            try await Task.sleep(nanoseconds: UInt64(timeoutSeconds * 1_000_000_000))
            var newState = timeoutState
            newState.statusMessage = "❌ Processing timed out after \(timeoutSeconds) seconds"
            newState.isProcessing = false
            newState.processingStage = "Timed out"
            req.logger.error("[DEBUG] Photogrammetry processing timed out after \(timeoutSeconds) seconds")
            await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: newState)
            try? await self.cleanup(tempFolder: inputFolder, logger: req.logger, req: req)
            throw Abort(.internalServerError, reason: "Processing timed out after \(timeoutSeconds) seconds")
        }

        // Periodic health check to confirm session is active
        let healthCheckTask = Task {
            while !Task.isCancelled {
                req.logger.info("[DEBUG] Session health check: progress=\(state.progress), stage=\(state.processingStage)")
                try await Task.sleep(nanoseconds: 30_000_000_000) // 30 seconds
            }
        }

        // Process session outputs in a background task
        let processingTask = Task.detached(priority: .userInitiated) { [weak self] in
            guard let self = self else { return }
            do {
                req.logger.info("[DEBUG] Starting to process photogrammetry session outputs")
                for try await output in session.outputs {
                    let updatedState = await self.handleSessionOutput(
                        output,
                        currentState: state,
                        req: req,
                        inputFolder: inputFolder,
                        outputURL: outputURL
                    )
                    await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: updatedState)
                    state = updatedState
                }
            } catch {
                await MainActor.run {
                    var errorState = state
                    errorState.statusMessage = "❌ Processing failed: \(error.localizedDescription)"
                    errorState.isProcessing = false
                    self.updateProcessingStage(errorState.progress, state: &errorState, req: req)
                    req.logger.error("[DEBUG] Photogrammetry session failed: \(error)")
                    Task {
                        await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: errorState)
                        do {
                            try await self.cleanup(tempFolder: inputFolder, logger: req.logger, req: req)
                        } catch {
                            req.logger.error("Cleanup failed: \(error)")
                        }
                    }
                    timeoutTask.cancel()
                    healthCheckTask.cancel()
                }
            }
        }

        req.logger.info("[DEBUG] Submitting photogrammetry request for model file at \(outputURL.path)")
        let requestStart = Date()
        do {
            try session.process(requests: [.modelFile(url: outputURL, detail: optimalDetailLevel)])
            let requestDuration = Date().timeIntervalSince(requestStart)
            req.logger.info("[DEBUG] Photogrammetry request submitted, duration: \(requestDuration)s")
            
            // Wait for processing to complete
            try await processingTask.value
            
            return state
        } catch {
            var errorState = state
            errorState.statusMessage = "❌ Failed to submit photogrammetry request: \(self.sanitizeError(error: error.localizedDescription))"
            errorState.isProcessing = false
            self.updateProcessingStage(errorState.progress, state: &errorState, req: req)
            req.logger.error("[DEBUG] Photogrammetry request submission failed: \(error)")
            await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: errorState)
            try await self.cleanup(tempFolder: inputFolder, logger: req.logger, req: req)
            timeoutTask.cancel()
            healthCheckTask.cancel()
            throw error
        }
    }

    // MARK: - Session Output Handler
    private func handleSessionOutput(
        _ output: PhotogrammetrySession.Output,
        currentState: ProcessingState,
        req: Request,
        inputFolder: URL,
        outputURL: URL
    ) async -> ProcessingState {
        var newState = currentState
        await MainActor.run {
            switch output {
            case .processingComplete:
                newState.statusMessage = "✅ Reconstruction complete!"
                newState.isProcessing = false
                self.updateProcessingStage(1.0, state: &newState, req: req)
                req.logger.info("[DEBUG] Photogrammetry processing complete")
                
            case .requestComplete(_, let result):
                if case .modelFile(url: let url) = result {
                    req.logger.info("[DEBUG] Model file generated at \(url.path), moving to \(outputURL.path)")
                    do {
                        try FileManager.default.moveItem(at: url, to: outputURL)
                        newState.statusMessage = "✅ Model saved to \(url.lastPathComponent)"
                        newState.isProcessing = false
                        self.updateProcessingStage(1.0, state: &newState, req: req)
                        req.logger.info("[DEBUG] Model moved to output URL, processing finished")
                    } catch {
                        newState.statusMessage = "❌ Failed to move model file: \(error.localizedDescription)"
                        newState.isProcessing = false
                        self.updateProcessingStage(newState.progress, state: &newState, req: req)
                        req.logger.error("[DEBUG] Failed to move model file: \(error)")
                    }
                }
                
            case .requestProgress(_, let fractionComplete):
                newState.progress = fractionComplete
                self.updateProcessingStage(fractionComplete, state: &newState, req: req)
                req.logger.info("[DEBUG] Processing progress: \(fractionComplete * 100)% - stage: \(newState.processingStage)")
                
            case .requestError(_, let error):
                newState.statusMessage = "❌ Error: \(self.sanitizeError(error: error.localizedDescription))"
                newState.isProcessing = false
                self.updateProcessingStage(newState.progress, state: &newState, req: req)
                req.logger.error("[DEBUG] Photogrammetry error: \(error.localizedDescription)")
                
            case .inputComplete:
                self.updateProcessingStage(newState.progress, state: &newState, req: req)
                req.logger.info("[DEBUG] Input data fully processed")
                
            case .invalidSample(let id, let reason):
                self.updateProcessingStage(newState.progress, state: &newState, req: req)
                req.logger.warning("[DEBUG] Invalid sample ID \(id): \(reason)")
                
            case .skippedSample(let id):
                self.updateProcessingStage(newState.progress, state: &newState, req: req)
                req.logger.info("[DEBUG] Skipped sample ID \(id)")
                
            case .automaticDownsampling:
                self.updateProcessingStage(newState.progress, state: &newState, req: req)
                req.logger.info("[DEBUG] Automatic downsampling triggered")
                
            default:
                req.logger.warning("[DEBUG] Unknown photogrammetry output received: \(String(describing: output))")
            }
        }
        return newState
    }

    // MARK: - Cleanup Function
    private func cleanup(tempFolder: URL, logger: Logger, req: Request) async throws {
        logger.info("[DEBUG] Starting cleanup of temporary folder: \(tempFolder.path)")
        do {
            // First try proper cleanup
            try FileManager.default.removeItem(at: tempFolder)
            logger.info("[DEBUG] Removed temporary folder: \(tempFolder.path)")
            
            // Then clean any remaining temporary files
            let tempDir = FileManager.default.temporaryDirectory
            let contents = try FileManager.default.contentsOfDirectory(at: tempDir, includingPropertiesForKeys: nil)
            for item in contents where item.lastPathComponent.contains("photogrammetry_") {
                try? FileManager.default.removeItem(at: item)
                logger.info("[DEBUG] Removed residual photogrammetry folder: \(item.path)")
            }
            
            // Update state
            if var state = req.storage[ProcessingStateKey.self] {
                state.isProcessing = false
                state.photogrammetrySession = nil
                logger.info("[DEBUG] Cleared processing state")
                await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
            }
        } catch {
            logger.error("Cleanup failed: \(error)")
            throw error
        }
    }

    // MARK: - Processing Logic
    private func shouldUsePLYReference(from folderURL: URL, req: Request, state: inout ProcessingState) async throws -> Bool {
        guard let plyURL = getPLYFile(from: folderURL, req: req) else {
            req.logger.info("[DEBUG] No PLY file found in \(folderURL.path)")
            return false
        }
        req.logger.info("[DEBUG] Found PLY file at \(plyURL.path), validating")
        do {
            let result = try validatePLYFile(plyURL, state: &state, req: req)
            req.logger.info("[DEBUG] PLY validation result: \(result)")
            await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
            return result
        } catch {
            req.logger.error("PLY validation failed: \(error.localizedDescription)")
            throw error
        }
    }

    private func prepareInputFolder(_ folderURL: URL, usePLY: Bool, req: Request) throws -> (tempFolder: URL, imagesFolder: URL) {
        let tempFolder = FileManager.default.temporaryDirectory
            .appendingPathComponent("photogrammetry_input_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempFolder, withIntermediateDirectories: true)
        req.logger.info("[DEBUG] Created temporary folder for photogrammetry input: \(tempFolder.path)")

        guard let imagesFolder = getImagesFolder(from: folderURL, req: req) else {
            req.logger.error("Failed to find images folder in \(folderURL.path)")
            throw Abort(.badRequest, reason: "Missing images folder. Ensure the ZIP contains an 'images' folder with at least 10 JPEG images and corresponding pose files.")
        }
        req.logger.info("[DEBUG] Found images folder: \(imagesFolder.path)")

        req.logger.info("[DEBUG] Processing images with pose files from \(imagesFolder.path)")
        let processedImages = try processImagesWithPoses(from: imagesFolder, req: req)
        let imagesDestFolder = tempFolder.appendingPathComponent("images")
        try FileManager.default.createDirectory(at: imagesDestFolder, withIntermediateDirectories: true)
        req.logger.info("[DEBUG] Created images destination folder: \(imagesDestFolder.path)")

        req.logger.info("[DEBUG] Copying processed images to \(imagesDestFolder.path)")
        try FileManager.default.copyContentsOfDirectory(at: processedImages, to: imagesDestFolder)
        req.logger.info("[DEBUG] Copied processed images to \(imagesDestFolder.path)")

        if usePLY, let plyFile = getPLYFile(from: folderURL, req: req) {
            let plyDest = tempFolder.appendingPathComponent("model.ply")
            req.logger.info("[DEBUG] Copying PLY file from \(plyFile.path) to \(plyDest.path)")
            try FileManager.default.copyItem(at: plyFile, to: plyDest)
            req.logger.info("[DEBUG] Creating PLY metadata file in \(tempFolder.path)")
            try createPLYMetadataFile(in: tempFolder, req: req)
        }

        return (tempFolder, imagesFolder)
    }

    private func processImagesWithPoses(from folderURL: URL, req: Request) throws -> URL {
        let tempFolder = FileManager.default.temporaryDirectory
            .appendingPathComponent("processed_images_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempFolder, withIntermediateDirectories: true)
        req.logger.info("[DEBUG] Created temporary folder for processed images: \(tempFolder.path)")

        let contents = try FileManager.default.contentsOfDirectory(at: folderURL, includingPropertiesForKeys: nil)
        let imageFiles = contents.filter {
            let ext = $0.pathExtension.lowercased()
            return ext == "jpg" || ext == "jpeg"
        }
        req.logger.info("[DEBUG] Found \(imageFiles.count) image files in \(folderURL.path)")
        
        var processedCount = 0
        var skippedDueToQuality = 0
        
        for imageURL in imageFiles {
            req.logger.info("[DEBUG] Processing image: \(imageURL.lastPathComponent)")
            let baseName = imageURL.deletingPathExtension().lastPathComponent
            let poseURL = folderURL.appendingPathComponent("\(baseName)_pose.json")
            
            guard FileManager.default.fileExists(atPath: poseURL.path) else {
                req.logger.warning("Missing pose file for \(imageURL.lastPathComponent)")
                continue
            }
            
            do {
                req.logger.info("[DEBUG] Reading pose file: \(poseURL.lastPathComponent)")
                let poseData = try Data(contentsOf: poseURL)
                let poseJson = try JSONSerialization.jsonObject(with: poseData) as? [String: Any]
                
                if let confidence = poseJson?["confidence"] as? Double, confidence < minPoseConfidence {
                    req.logger.info("[DEBUG] Skipping \(imageURL.lastPathComponent): confidence (\(confidence)) < \(minPoseConfidence)")
                    skippedDueToQuality += 1
                    continue
                }
                if let count = poseJson?["feature_count"] as? Int, count < minFeatureCount {
                    req.logger.info("[DEBUG] Skipping \(imageURL.lastPathComponent): feature count (\(count)) < \(minFeatureCount)")
                    skippedDueToQuality += 1
                    continue
                }
                if let state = poseJson?["tracking_state"] as? String, state != "Normal" {
                    req.logger.info("[DEBUG] Skipping \(imageURL.lastPathComponent): tracking state = \(state)")
                    skippedDueToQuality += 1
                    continue
                }
                
                req.logger.info("[DEBUG] Validating pose JSON for \(imageURL.lastPathComponent)")
                let poseMatrix = try validatePoseJSON(poseData, req: req)
                let transform = convertToSIMDMatrix(poseMatrix, req: req)
                let outputURL = tempFolder.appendingPathComponent(imageURL.lastPathComponent)
                
                req.logger.info("[DEBUG] Embedding pose data into \(outputURL.lastPathComponent)")
                let embedStart = Date()
                try embedPoseData(in: imageURL, to: outputURL, with: transform, poseJson: poseJson, req: req)
                let embedDuration = Date().timeIntervalSince(embedStart)
                req.logger.info("[DEBUG] Embedded pose data for \(imageURL.lastPathComponent), duration: \(embedDuration)s")
                processedCount += 1
                
            } catch {
                req.logger.warning("Error processing \(imageURL.lastPathComponent): \(error)")
            }
        }
        
        req.logger.info("Processed \(processedCount) images, skipped \(skippedDueToQuality) due to quality")
        
        guard processedCount >= 10 else {
            throw Abort(.badRequest, reason: """
                Need at least 10 valid images (found \(processedCount)). 
                \(skippedDueToQuality > 0 ? "\(skippedDueToQuality) were skipped due to low quality." : "")
                """)
        }
        
        return tempFolder
    }

    // MARK: - Helper Functions
    private func validatePoseJSON(_ data: Data, req: Request) throws -> [[Double]] {
        req.logger.info("[DEBUG] Validating pose JSON")
        guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let matrix = json["transform"] as? [[Double]] else {
            throw Abort(.badRequest, reason: "Missing or invalid transform matrix")
        }
        
        // Matrix structure validation
        guard matrix.count == 4, matrix.allSatisfy({ $0.count == 4 }) else {
            throw Abort(.badRequest, reason: "Matrix must be 4x4")
        }
        
        // Numerical validation
        for row in matrix {
            for value in row {
                if value.isNaN || value.isInfinite {
                    throw Abort(.badRequest, reason: "Matrix contains invalid numbers")
                }
            }
        }
        
        // Transform validation
        let simdMatrix = convertToSIMDMatrix(matrix, req: req)
        let determinant = abs(simdMatrix.determinant)
        guard determinant > 1e-6 else {
            throw Abort(.badRequest, reason: "Degenerate transform matrix")
        }
        
        req.logger.info("[DEBUG] Pose JSON validated successfully")
        return matrix
    }

    private func embedPoseData(in sourceURL: URL, to destinationURL: URL, with transform: simd_float4x4, poseJson: [String: Any]?, req: Request) throws {
        req.logger.info("[DEBUG] Embedding pose data into image from \(sourceURL.path) to \(destinationURL.path)")
        guard let source = CGImageSourceCreateWithURL(sourceURL as CFURL, nil) else {
            throw Abort(.internalServerError, reason: "Failed to create image source")
        }

        var imageData = try Data(contentsOf: sourceURL)
        req.logger.info("[DEBUG] Loaded image data, size: \(imageData.count) bytes")
        if let cgImage = CGImageSourceCreateImageAtIndex(source, 0, nil) {
            let originalWidth = cgImage.width
            let originalHeight = cgImage.height
            let maxDimension = 1920
            req.logger.info("[DEBUG] Image dimensions: \(originalWidth)x\(originalHeight)")

            if originalWidth > maxDimension || originalHeight > maxDimension {
                req.logger.info("[DEBUG] Resizing image to max dimension \(maxDimension)")
                let scale = CGFloat(maxDimension) / CGFloat(max(originalWidth, originalHeight))
                let newWidth = Int(CGFloat(originalWidth) * scale)
                let newHeight = Int(CGFloat(originalHeight) * scale)

                if let context = CGContext(
                    data: nil,
                    width: newWidth,
                    height: newHeight,
                    bitsPerComponent: cgImage.bitsPerComponent,
                    bytesPerRow: 0,
                    space: cgImage.colorSpace ?? CGColorSpaceCreateDeviceRGB(),
                    bitmapInfo: cgImage.bitmapInfo.rawValue
                ) {
                    context.interpolationQuality = .high
                    context.draw(cgImage, in: CGRect(x: 0, y: 0, width: newWidth, height: newHeight))
                    if let resizedImage = context.makeImage(),
                       let resizedData = CGImageDestinationCreateDataWithOptions(image: resizedImage, type: .jpeg, count: 1, options: [kCGImageDestinationLossyCompressionQuality as CFString: 0.8], req: req) {
                        imageData = resizedData
                        req.logger.info("[DEBUG] Resized image, new dimensions: \(newWidth)x\(newHeight), size: \(imageData.count) bytes")
                    }
                }
            }
        }

        guard let destination = CGImageDestinationCreateWithURL(destinationURL as CFURL, UTType.jpeg.identifier as CFString, 1, nil) else {
            throw Abort(.internalServerError, reason: "Failed to create image destination")
        }

        var metadata = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [String: Any] ?? [:]
        var exifDict = metadata[kCGImagePropertyExifDictionary as String] as? [String: Any] ?? [:]

        let transformDict: [String: [Float]] = [
            "row0": [transform.columns.0.x, transform.columns.0.y, transform.columns.0.z, transform.columns.0.w],
            "row1": [transform.columns.1.x, transform.columns.1.y, transform.columns.1.z, transform.columns.1.w],
            "row2": [transform.columns.2.x, transform.columns.2.y, transform.columns.2.z, transform.columns.2.w],
            "row3": [transform.columns.3.x, transform.columns.3.y, transform.columns.3.z, transform.columns.3.w]
        ]
        exifDict["CameraTransform"] = transformDict

        if let poseJson = poseJson {
            exifDict["ARKitMetadata"] = [
                "confidence": poseJson["confidence"] as? Double ?? 0.0,
                "tracking_state": poseJson["tracking_state"] as? String ?? "Unknown",
                "feature_count": poseJson["feature_count"] as? Int ?? 0,
                "timestamp": poseJson["timestamp"] as? Double ?? 0.0,
                "light_estimate": poseJson["light_estimate"] as? Double ?? 0.0
            ]
            req.logger.info("[DEBUG] Added ARKit metadata to EXIF: confidence=\(poseJson["confidence"] ?? 0), tracking_state=\(poseJson["tracking_state"] ?? "Unknown")")
        }

        metadata[kCGImagePropertyExifDictionary as String] = exifDict

        if let source = CGImageSourceCreateWithData(imageData as CFData, nil) {
            CGImageDestinationAddImageFromSource(destination, source, 0, metadata as CFDictionary)
            guard CGImageDestinationFinalize(destination) else {
                throw Abort(.internalServerError, reason: "Failed to save image with pose data")
            }
            req.logger.info("[DEBUG] Saved image with embedded pose data to \(destinationURL.path)")
        }
    }

    private func createPLYMetadataFile(in folder: URL, req: Request) throws {
        req.logger.info("[DEBUG] Creating PLY metadata file in \(folder.path)")
        let metadata = """
        {
            "version": 2,
            "pointCloudWeight": 0.8,
            "useAsReference": true,
            "coordinateSystem": "camera",
            "poseQualityThreshold": \(minPoseConfidence),
            "minFeatureCount": \(minFeatureCount)
        }
        """
        try metadata.write(to: folder.appendingPathComponent("model.ply.json"), atomically: true, encoding: .utf8)
        req.logger.info("[DEBUG] PLY metadata file created at \(folder.appendingPathComponent("model.ply.json").path)")
    }

    private func convertToSIMDMatrix(_ matrix: [[Double]], req: Request) -> simd_float4x4 {
        req.logger.info("[DEBUG] Converting pose matrix to SIMD format")
        let result = simd_float4x4(
            SIMD4<Float>(Float(matrix[0][0]), Float(matrix[0][1]), Float(matrix[0][2]), Float(matrix[0][3])),
            SIMD4<Float>(Float(matrix[1][0]), Float(matrix[1][1]), Float(matrix[1][2]), Float(matrix[1][3])),
            SIMD4<Float>(Float(matrix[2][0]), Float(matrix[2][1]), Float(matrix[2][2]), Float(matrix[2][3])),
            SIMD4<Float>(Float(matrix[3][0]), Float(matrix[3][1]), Float(matrix[3][2]), Float(matrix[3][3]))
        )
        req.logger.info("[DEBUG] Converted matrix to SIMD format")
        return result
    }

    private func getImagesFolder(from folderURL: URL, req: Request) -> URL? {
        let fileManager = FileManager.default
        var queue = [folderURL]
        var checkedPaths = Set<URL>()

        req.logger.info("[DEBUG] Searching for images folder starting at \(folderURL.path)")
        while !queue.isEmpty {
            let currentURL = queue.removeFirst()
            if checkedPaths.contains(currentURL) { continue }
            checkedPaths.insert(currentURL)

            guard fileManager.fileExists(atPath: currentURL.path) else {
                req.logger.info("Folder not found: \(currentURL.path)")
                continue
            }

            do {
                let contents = try fileManager.contentsOfDirectory(at: currentURL, includingPropertiesForKeys: nil)
                req.logger.info("Files in \(currentURL.lastPathComponent): \(contents.map { $0.lastPathComponent })")
                
                let imageFiles = contents.filter {
                    let ext = $0.pathExtension.lowercased()
                    return ext == "jpg" || ext == "jpeg"
                }
                req.logger.info("Found \(imageFiles.count) image files in \(currentURL.lastPathComponent)")
                
                let hasPoseFiles = imageFiles.contains { imageURL in
                    let baseName = imageURL.deletingPathExtension().lastPathComponent
                    let poseURL = currentURL.appendingPathComponent("\(baseName)_pose.json")
                    let exists = fileManager.fileExists(atPath: poseURL.path)
                    req.logger.info("Pose file for \(imageURL.lastPathComponent): \(exists ? "Found" : "Missing")")
                    return exists
                }
                req.logger.info("Has pose files in \(currentURL.lastPathComponent): \(hasPoseFiles)")
                
                if imageFiles.count >= 10 {
                    req.logger.info("Valid images folder found: \(currentURL.path)")
                    return currentURL
                }
                
                let subdirs = contents.filter { $0.hasDirectoryPath }
                queue.append(contentsOf: subdirs)
                req.logger.info("Added subdirs to queue: \(subdirs.map { $0.lastPathComponent })")
            } catch {
                req.logger.error("Error accessing folder \(currentURL.path): \(error)")
            }
        }
        
        req.logger.error("No valid images folder found in \(folderURL.path)")
        return nil
    }

    private func getPLYFile(from folderURL: URL, req: Request) -> URL? {
        let plyURL = folderURL.appendingPathComponent("model.ply")
        guard FileManager.default.fileExists(atPath: plyURL.path) else {
            req.logger.info("[DEBUG] No PLY file found at \(plyURL.path)")
            return nil
        }

        do {
            let attributes = try FileManager.default.attributesOfItem(atPath: plyURL.path)
            let size = attributes[.size] as? Int64 ?? 0
            req.logger.info("[DEBUG] Found PLY file at \(plyURL.path), size: \(size) bytes")
            return size > 1024 ? plyURL : nil
        } catch {
            req.logger.error("Error checking PLY file attributes: \(error)")
            return nil
        }
    }

    private func validatePLYFile(_ plyURL: URL, state: inout ProcessingState, req: Request) throws -> Bool {
        req.logger.info("[DEBUG] Validating PLY file at \(plyURL.path)")
        do {
            let attributes = try FileManager.default.attributesOfItem(atPath: plyURL.path)
            let fileSize = (attributes[.size] as? Int64) ?? 0
            guard fileSize > 1024 else {
                state.statusMessage = "⚠️ PLY file too small (needs at least 1KB)"
                req.logger.info("[DEBUG] PLY file too small: \(fileSize) bytes")
                return false
            }
            
            let fileHandle = try FileHandle(forReadingFrom: plyURL)
            defer { fileHandle.closeFile() }
            let headerData = fileHandle.readData(ofLength: 1024)
            
            guard let header = String(data: headerData, encoding: .ascii) else {
                state.statusMessage = "⚠️ PLY header contains invalid characters"
                req.logger.info("[DEBUG] Invalid PLY header encoding")
                return false
            }
            
            // Check for required PLY components
            guard header.lowercased().hasPrefix("ply") else {
                state.statusMessage = "Invalid PLY file (missing header)"
                req.logger.info("[DEBUG] PLY file missing 'ply' header")
                return false
            }
            
            guard header.contains("element vertex") else {
                state.statusMessage = "PLY file missing vertex data"
                req.logger.info("[DEBUG] PLY missing vertex data")
                return false
            }
            
            // Extract vertex count
            var vertexCount = 0
            if let vertexRange = header.range(of: "element vertex") {
                let remainingHeader = header[vertexRange.upperBound...]
                if let countRange = remainingHeader.range(of: "\\d+", options: .regularExpression) {
                    vertexCount = Int(remainingHeader[countRange]) ?? 0
                }
            }
            req.logger.info("[DEBUG] PLY file vertex count: \(vertexCount)")
            
            guard vertexCount > 1000 else {
                state.statusMessage = "Error: PLY needs ≥1000 vertices (found: \(vertexCount))"
                req.logger.info("[DEBUG] PLY vertex count too low: \(vertexCount)")
                return false
            }
            
            // Check for position and normal data
            let hasPosition = header.contains("property float x") &&
                             header.contains("property float y") &&
                             header.contains("property float z")
            
            let hasNormals = header.contains("property float nx") &&
                            header.contains("property float ny") &&
                            header.contains("property float nz")
            
            if !hasPosition {
                state.statusMessage = "Error: PLY missing position data"
                req.logger.info("[DEBUG] PLY missing position data")
                return false
            }
            
            if !hasNormals {
                state.statusMessage = "Warning: PLY missing normals (may affect quality)"
                req.logger.warning("[DEBUG] PLY missing normals")
            }
            
            req.logger.info("[DEBUG] PLY file validated successfully")
            return true
        } catch {
            state.statusMessage = "Error encountered during PLY validation: \(sanitizeError(error: error.localizedDescription))"
            req.logger.error("[DEBUG] PLY validation error: \(error)")
            throw error
        }
    }

    private func cleanupTemporaryFolders(_ req: Request) {
        Task {
            req.logger.info("[DEBUG] Starting cleanup of all temporary folders")
            let tempDir = FileManager.default.temporaryDirectory
            do {
                let contents = try FileManager.default.contentsOfDirectory(at: tempDir, includingPropertiesForKeys: nil)
                for item in contents where item.lastPathComponent.contains("photogrammetry_") {
                    try? FileManager.default.removeItem(at: item)
                    req.logger.info("[DEBUG] Removed temporary folder: \(item.path)")
                }
            } catch {
                req.logger.error("Cleanup error: \(error)")
            }
        }
    }

    private func sanitizeError(error: String) -> String {
        return error
            .replacingOccurrences(of: "The operation couldn't be completed. ", with: "")
            .replacingOccurrences(of: "(com.apple.photogrammetry ", with: "")
    }

    private func CGImageDestinationCreateDataWithOptions(image: CGImage, type: UTType, count: Int, options: [CFString: Any], req: Request) -> Data? {
        req.logger.info("[DEBUG] Creating image data with options for type: \(type.identifier)")
        let data = NSMutableData()
        guard let destination = CGImageDestinationCreateWithData(data as CFMutableData, type.identifier as CFString, count, nil) else {
            req.logger.error("[DEBUG] Failed to create image destination")
            return nil
        }
        CGImageDestinationAddImage(destination, image, options as CFDictionary)
        let success = CGImageDestinationFinalize(destination)
        req.logger.info("[DEBUG] Image data creation \(success ? "succeeded" : "failed")")
        return success ? data as Data : nil
    }

    private func updateProcessingStage(_ progress: Double, state: inout ProcessingState, req: Request) {
        switch progress {
        case 0..<0.2: state.processingStage = "Processing images..."
        case 0.2..<0.5: state.processingStage = "Aligning point cloud..."
        case 0.5..<0.8: state.processingStage = "Generating geometry..."
        case 0.8..<1.0: state.processingStage = "Finalizing model..."
        default: state.processingStage = "Processing..."
        }
        req.logger.info("[DEBUG] Updated processing stage: \(state.processingStage)")
    }
}

// MARK: - FileManager Extension
extension FileManager {
    func copyContentsOfDirectory(at srcURL: URL, to dstURL: URL) throws {
        try createDirectory(at: dstURL, withIntermediateDirectories: true)
        let contents = try contentsOfDirectory(at: srcURL, includingPropertiesForKeys: nil)
        for item in contents {
            try copyItem(at: item, to: dstURL.appendingPathComponent(item.lastPathComponent))
        }
    }
}
