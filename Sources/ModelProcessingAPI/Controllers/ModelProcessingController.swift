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

// Response Struct for Model URL and Expiration
struct ModelResponse: Content {
    let modelUrl: String
    let statusMessage: String
    let expiresAt: String
}

// Error Response Struct
struct APIError: Content, Error {
    let message: String
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

struct FileUpload: Content {
    var data: ByteBuffer
    var filename: String
}

@MainActor
final class ModelProcessingController {
    private let optimalDetailLevel: PhotogrammetrySession.Request.Detail = .raw
    private let minPoseConfidence: Double = 0.3
    private let minFeatureCount: Int = 10
    private let maxFileSize: Int = 600_000_000
    private let maxConcurrentTasks: Int = 8
    private let baseUrl = "http://213.73.97.120"
    private let retentionPeriod: TimeInterval = 24 * 3600

    func processModel(_ req: Request) async throws -> Response {
        let activeTasks = req.application.storage[ActiveTasksKey.self] ?? 0
        guard activeTasks < maxConcurrentTasks else {
            let error = APIError(message: "Server is busy, please try again later")
            let data = try JSONEncoder().encode(error)
            return Response(status: .tooManyRequests, body: .init(data: data))
        }
        req.application.storage[ActiveTasksKey.self] = activeTasks + 1
        defer { req.application.storage[ActiveTasksKey.self] = max(0, activeTasks - 1) }

        let file: File
        do {
            file = try await getUploadedFile(req: req)
        } catch {
            let error = APIError(message: "No file uploaded or invalid field name")
            let data = try JSONEncoder().encode(error)
            return Response(status: .badRequest, body: .init(data: data))
        }

        guard file.data.readableBytes < maxFileSize else {
            let error = APIError(message: "Uploaded file exceeds 600MB limit")
            let data = try JSONEncoder().encode(error)
            return Response(status: .payloadTooLarge, body: .init(data: data))
        }

        let fileManager = FileManager.default
        let tempDir = fileManager.temporaryDirectory.appendingPathComponent("model_processing_\(UUID().uuidString)")
        let inputDir = tempDir.appendingPathComponent("input")
        let outputDir = tempDir.appendingPathComponent("output")
        let publicModelsDir = URL(fileURLWithPath: "/tmp/models")

        // Robust directory creation with verification
        do {
            try fileManager.createDirectory(at: inputDir, withIntermediateDirectories: true)
            try fileManager.createDirectory(at: outputDir, withIntermediateDirectories: true)
            try fileManager.createDirectory(at: publicModelsDir, withIntermediateDirectories: true)

            // Verify directories were actually created
            var isDir: ObjCBool = false
            guard fileManager.fileExists(atPath: inputDir.path, isDirectory: &isDir), isDir.boolValue else {
                throw APIError(message: "Failed to create input directory")
            }
            guard fileManager.fileExists(atPath: outputDir.path, isDirectory: &isDir), isDir.boolValue else {
                throw APIError(message: "Failed to create output directory")
            }
            guard fileManager.fileExists(atPath: publicModelsDir.path, isDirectory: &isDir), isDir.boolValue else {
                throw APIError(message: "Failed to create models directory")
            }

            req.logger.info("Successfully created all required directories")
        } catch {
            req.logger.error("Directory creation failed: \(error)")
            let apiError = APIError(message: "Failed to create directories: \(error.localizedDescription)")
            let data = try JSONEncoder().encode(apiError)
            return Response(status: .internalServerError, body: .init(data: data))
        }

        let zipURL = inputDir.appendingPathComponent(file.filename.lowercased().hasSuffix(".zip") ? file.filename : "\(file.filename).zip")

        // Ensure directory exists before writing file
        if !fileManager.fileExists(atPath: inputDir.path) {
            try fileManager.createDirectory(at: inputDir, withIntermediateDirectories: true)
        }

        do {
            try await req.fileio.writeFile(file.data, at: zipURL.path)
            req.logger.debug("Saved uploaded ZIP to: \(zipURL.path)")

            // Verify file was written
            guard fileManager.fileExists(atPath: zipURL.path) else {
                throw APIError(message: "Failed to verify file write operation")
            }

            try extractZip(zipURL, to: inputDir, req: req)

            let state = ProcessingState(selectedFolderURL: inputDir)
            await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)

            let tempOutputFileURL = outputDir.appendingPathComponent("output_model.usdz")
            let response = try await startProcessing(outputURL: tempOutputFileURL, req: req, state: state)

            // Schedule cleanup of temp directory
            let tempDirPath = tempDir.path
            Task.detached { @Sendable in
                do {
                    try FileManager.default.removeItem(atPath: tempDirPath)
                    req.logger.info("Successfully cleaned up temporary directory: \(tempDirPath)")
                } catch {
                    req.logger.error("Background cleanup failed: \(error)")
                }
            }

            req.logger.info("Returning successful response")
            let data = try JSONEncoder().encode(response)
            return Response(status: .ok, body: .init(data: data))
        } catch {
            // Cleanup on failure
            do {
                try fileManager.removeItem(at: tempDir)
                req.logger.info("Cleaned up temporary directory after failure")
            } catch {
                req.logger.error("Cleanup failed: \(error)")
            }

            let errorMessage: String
            if let apiError = error as? APIError {
                errorMessage = apiError.message
            } else {
                errorMessage = error.localizedDescription.contains("Missing images folder") ?
                    error.localizedDescription :
                    "Processing failed: \(error.localizedDescription)"
            }

            req.logger.error("Processing failed: \(errorMessage)")
            let apiError = APIError(message: errorMessage)
            let data = try JSONEncoder().encode(apiError)
            return Response(status: .internalServerError, body: .init(data: data))
        }
    }

    func getProgress(_ req: Request) async throws -> ProgressResponse {
        guard let state = req.storage[ProcessingStateKey.self] else {
            throw APIError(message: "No active processing session")
        }
        return ProgressResponse(
            statusMessage: state.statusMessage,
            progress: state.progress,
            processingStage: state.processingStage,
            isProcessing: state.isProcessing
        )
    }

    private func getUploadedFile(req: Request) async throws -> File {
        req.logger.debug("Attempting to retrieve uploaded file")

        // Debug: Log raw request body
        if let bodyData = req.body.data {
            let debugRawPath = "/tmp/upload_debug_\(UUID().uuidString).bin"
            do {
                try await req.fileio.writeFile(bodyData, at: debugRawPath)
                req.logger.debug("Saved raw request body for debugging: \(debugRawPath), size: \(bodyData.readableBytes)")
            } catch {
                req.logger.error("Failed to save raw body debug file: \(error)")
            }
        }

        // Try multipart form with 'file' field first
        if let formFile = try? req.content.decode(FileUpload.self) {
            req.logger.debug("Found file in 'file' field: \(formFile.filename), size: \(formFile.data.readableBytes)")
            return File(data: formFile.data, filename: formFile.filename)
        }
        // Then try 'data' field
        else if let formFile = try? req.content.decode(FileUpload.self) {
            req.logger.debug("Found file in 'data' field: \(formFile.filename), size: \(formFile.data.readableBytes)")
            return File(data: formFile.data, filename: formFile.filename)
        }
        // Then try direct file upload
        else if let uploadedFile = try? req.content.decode(File.self) {
            req.logger.debug("Found direct file upload: \(uploadedFile.filename), size: \(uploadedFile.data.readableBytes)")
            return uploadedFile
        }
        // Finally fall back to raw body
        else if let bodyData = req.body.data {
            let filename = req.headers.contentDisposition?.filename ??
                         req.headers.first(name: "filename") ??
                         "upload_\(UUID().uuidString).zip"

            req.logger.debug("Using raw body as file: \(filename), size: \(bodyData.readableBytes)")
            return File(data: bodyData, filename: filename)
        }

        req.logger.error("Could not locate file in request")
        throw APIError(message: "No file found in request. Please upload a file using 'file' or 'data' field, or as raw body.")
    }

    private func extractZip(_ zipURL: URL, to destination: URL, req: Request, depth: Int = 0) throws {
        guard depth < 3 else {
            throw APIError(message: "Maximum ZIP nesting depth (3) exceeded")
        }

        let fileManager = FileManager.default
        req.logger.notice("Extracting ZIP archive: \(zipURL.lastPathComponent)")

        do {
            let archive = try Archive(url: zipURL, accessMode: .read)

            // Log contents for debugging
            req.logger.debug("Archive contents:")
            for entry in archive {
                req.logger.debug("- \(entry.path)")
            }

            // Perform extraction
            try fileManager.unzipItem(at: zipURL, to: destination)

            // Verify extraction
            let contents = try fileManager.contentsOfDirectory(at: destination, includingPropertiesForKeys: nil)
            req.logger.notice("Extracted \(contents.count) items")

            // Handle nested ZIPs
            for item in contents where item.pathExtension.lowercased() == "zip" &&
                                      item.lastPathComponent != zipURL.lastPathComponent {
                req.logger.debug("Found nested ZIP: \(item.lastPathComponent)")

                let nestedDest = destination.appendingPathComponent(item.deletingPathExtension().lastPathComponent)
                try fileManager.createDirectory(at: nestedDest, withIntermediateDirectories: true)

                // Recursively extract
                try extractZip(item, to: nestedDest, req: req, depth: depth + 1)

                // Remove the nested ZIP after extraction
                try fileManager.removeItem(at: item)
            }
        } catch {
            req.logger.error("ZIP extraction failed: \(error)")
            throw APIError(message: "Failed to extract ZIP archive: \(error.localizedDescription)")
        }
    }

    private func startProcessing(outputURL: URL, req: Request, state: ProcessingState) async throws -> ModelResponse {
        var state = state
        guard let folderURL = state.selectedFolderURL else {
            throw APIError(message: "No input folder available for processing")
        }

        // Update processing state
        state.isProcessing = true
        state.progress = 0.0
        state.statusMessage = "Initializing processing"
        state.processingStage = "Preparing data"
        await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)

        // Check for PLY reference file
        let usePLYAsReference = try await shouldUsePLYReference(from: folderURL, req: req, state: &state)

        // Prepare input folder structure
        let (inputFolder, imagesFolder) = try prepareInputFolder(folderURL, usePLY: usePLYAsReference, req: req)

        // Configure photogrammetry session
        var configuration = PhotogrammetrySession.Configuration()
        configuration.sampleOrdering = .sequential
        configuration.featureSensitivity = usePLYAsReference ? .high : .normal
        configuration.isObjectMaskingEnabled = false

        // Create and start session
        let session = try PhotogrammetrySession(input: imagesFolder, configuration: configuration)
        state.photogrammetrySession = session
        await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)

        // Setup timeout
        let timeoutTask = Task {
            try await Task.sleep(nanoseconds: 1_200_000_000_000) // 20 minutes
            session.finish()
            throw APIError(message: "Processing timed out after 20 minutes")
        }

        do {
            // Start processing
            try session.process(requests: [.modelFile(url: outputURL, detail: optimalDetailLevel)])
            var response: ModelResponse?

            // Process outputs
            for try await output in session.outputs {
                let (updatedState, resp) = try await handleSessionOutput(
                    output,
                    currentState: state,
                    req: req,
                    inputFolder: inputFolder,
                    outputURL: outputURL
                )
                state = updatedState
                if let resp = resp {
                    response = resp
                    break
                }
                await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
            }

            // Cleanup
            timeoutTask.cancel()
            session.finish()

            guard let response = response else {
                throw APIError(message: "Processing completed but no model was generated")
            }

            return response
        } catch {
            // Error handling
            timeoutTask.cancel()
            session.finish()
            try await cleanup(tempFolder: inputFolder, logger: req.logger, req: req)
            throw error
        }
    }

    private func handleSessionOutput(
        _ output: PhotogrammetrySession.Output,
        currentState: ProcessingState,
        req: Request,
        inputFolder: URL,
        outputURL: URL
    ) async throws -> (ProcessingState, ModelResponse?) {
        var newState = currentState
        var response: ModelResponse? = nil

        switch output {
        case .processingComplete:
            newState.statusMessage = "Processing complete"
            newState.isProcessing = false
            newState.progress = 1.0
            req.logger.info("Processing completed successfully")

        case .requestComplete(_, let result):
            if case .modelFile(url: let url) = result {
                do {
                    // Move the generated file to output location
                    try FileManager.default.moveItem(at: url, to: outputURL)

                    // Prepare final output location
                    let publicModelsDir = URL(fileURLWithPath: "/tmp/models")
                    let outputFileName = "model_\(UUID().uuidString).usdz"
                    let finalOutputFileURL = publicModelsDir.appendingPathComponent(outputFileName)

                    // Move to public directory
                    try FileManager.default.moveItem(at: outputURL, to: finalOutputFileURL)
                    req.logger.info("Model saved to: \(finalOutputFileURL.path)")

                    // Generate response
                    let modelUrl = "\(baseUrl)/models/\(outputFileName)"
                    let expirationDate = Date().addingTimeInterval(retentionPeriod)
                    let dateFormatter = ISO8601DateFormatter()
                    dateFormatter.timeZone = TimeZone(identifier: "Asia/Karachi")
                    let expiresAt = dateFormatter.string(from: expirationDate)

                    response = ModelResponse(
                        modelUrl: modelUrl,
                        statusMessage: "Model processed successfully",
                        expiresAt: expiresAt
                    )

                    newState.statusMessage = "Model saved"
                    newState.isProcessing = false
                } catch {
                    req.logger.error("File operation failed: \(error)")
                    newState.statusMessage = "Error saving model"
                    newState.isProcessing = false
                    throw APIError(message: "Failed to save model: \(error.localizedDescription)")
                }
            }

        case .requestError(_, let error):
            newState.statusMessage = "Error: \(sanitizeError(error: error.localizedDescription))"
            newState.isProcessing = false
            req.logger.error("Processing error: \(error.localizedDescription)")

        case .invalidSample(let id, let reason):
            req.logger.warning("Invalid sample (ID: \(id)): \(reason)")

        case .processingCancelled:
            newState.statusMessage = "Processing cancelled"
            newState.isProcessing = false
            req.logger.warning("Processing was cancelled")

        default:
            break
        }

        return (newState, response)
    }

    private func updateProcessingStage(_ progress: Double, state: inout ProcessingState, req: Request) {
        let newStage: String
        switch progress {
        case 0..<0.2: newStage = "Processing images"
        case 0.2..<0.5: newStage = "Aligning point cloud"
        case 0.5..<0.8: newStage = "Generating geometry"
        case 0.8..<1.0: newStage = "Finalizing model"
        default: newStage = "Processing"
        }

        if state.processingStage != newStage {
            state.processingStage = newStage
            req.logger.info("Progress update: \(newStage) (\(Int(progress * 100))%)")
        }
    }

    private func cleanup(tempFolder: URL, logger: Logger, req: Request) async throws {
        do {
            try FileManager.default.removeItem(at: tempFolder)
            logger.info("Cleaned up temporary folder: \(tempFolder.path)")

            if var state = req.storage[ProcessingStateKey.self] {
                state.isProcessing = false
                state.photogrammetrySession = nil
                await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
            }
        } catch {
            logger.error("Cleanup failed: \(error)")
            throw APIError(message: "Failed to clean up temporary files: \(error.localizedDescription)")
        }
    }

    private func shouldUsePLYReference(from folderURL: URL, req: Request, state: inout ProcessingState) async throws -> Bool {
        guard let plyURL = getPLYFile(from: folderURL, req: req) else {
            return false
        }

        do {
            let result = try validatePLYFile(plyURL, state: &state, req: req)
            await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
            return result
        } catch {
            throw APIError(message: "PLY validation failed: \(error.localizedDescription)")
        }
    }

    private func prepareInputFolder(_ folderURL: URL, usePLY: Bool, req: Request) throws -> (tempFolder: URL, imagesFolder: URL) {
        let tempFolder = FileManager.default.temporaryDirectory
            .appendingPathComponent("photogrammetry_input_\(UUID().uuidString)")

        try FileManager.default.createDirectory(at: tempFolder, withIntermediateDirectories: true)
        req.logger.debug("Created temporary input folder: \(tempFolder.path)")

        guard let imagesFolder = getImagesFolder(from: folderURL, req: req) else {
            throw APIError(message: "No valid images folder found. Ensure your ZIP contains an 'images' folder with at least 10 JPEG images and corresponding pose files.")
        }

        // Process images with pose data
        let processedImages = try processImagesWithPoses(from: imagesFolder, req: req)
        let imagesDestFolder = tempFolder.appendingPathComponent("images")
        try FileManager.default.createDirectory(at: imagesDestFolder, withIntermediateDirectories: true)
        try FileManager.default.copyContentsOfDirectory(at: processedImages, to: imagesDestFolder)

        // Handle PLY reference file if available
        if usePLY, let plyFile = getPLYFile(from: folderURL, req: req) {
            let plyDest = tempFolder.appendingPathComponent("model.ply")
            try FileManager.default.copyItem(at: plyFile, to: plyDest)
            try createPLYMetadataFile(in: tempFolder, req: req)
            req.logger.info("Using PLY reference file for processing")
        }

        return (tempFolder, imagesFolder)
    }

    private func processImagesWithPoses(from folderURL: URL, req: Request) throws -> URL {
        let tempFolder = FileManager.default.temporaryDirectory
            .appendingPathComponent("processed_images_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempFolder, withIntermediateDirectories: true)

        let contents = try FileManager.default.contentsOfDirectory(at: folderURL, includingPropertiesForKeys: nil)
        let imageFiles = contents.filter {
            let ext = $0.pathExtension.lowercased()
            return ext == "jpg" || ext == "jpeg"
        }

        var processedCount = 0
        var skippedDueToQuality = 0

        for imageURL in imageFiles {
            let baseName = imageURL.deletingPathExtension().lastPathComponent
            let poseURL = folderURL.appendingPathComponent("\(baseName)_pose.json")

            guard FileManager.default.fileExists(atPath: poseURL.path) else {
                req.logger.debug("No pose file for image: \(imageURL.lastPathComponent)")
                continue
            }

            do {
                let poseData = try Data(contentsOf: poseURL)
                let poseJson = try JSONSerialization.jsonObject(with: poseData) as? [String: Any]

                // Quality checks
                if let confidence = poseJson?["confidence"] as? Double, confidence < minPoseConfidence {
                    skippedDueToQuality += 1
                    continue
                }
                if let count = poseJson?["feature_count"] as? Int, count < minFeatureCount {
                    skippedDueToQuality += 1
                    continue
                }
                if let state = poseJson?["tracking_state"] as? String, state != "Normal" {
                    skippedDueToQuality += 1
                    continue
                }

                // Process valid image
                let poseMatrix = try validatePoseJSON(poseData, req: req)
                let transform = convertToSIMDMatrix(poseMatrix, req: req)
                let outputURL = tempFolder.appendingPathComponent(imageURL.lastPathComponent)
                try embedPoseData(in: imageURL, to: outputURL, with: transform, poseJson: poseJson, req: req)
                processedCount += 1
            } catch {
                req.logger.debug("Skipping image due to error: \(error.localizedDescription)")
                continue
            }
        }

        guard processedCount >= 10 else {
            throw APIError(message: """
                Insufficient valid images (found \(processedCount), need at least 10). 
                \(skippedDueToQuality > 0 ? "\(skippedDueToQuality) images were skipped due to low quality." : "")
                """)
        }

        req.logger.info("Processed \(processedCount) valid images with pose data")
        return tempFolder
    }

    private func validatePoseJSON(_ data: Data, req: Request) throws -> [[Double]] {
        guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let matrix = json["transform"] as? [[Double]] else {
            throw APIError(message: "Invalid pose data: missing or malformed transform matrix")
        }

        // Validate matrix structure
        guard matrix.count == 4, matrix.allSatisfy({ $0.count == 4 }) else {
            throw APIError(message: "Invalid transform matrix: must be 4x4")
        }

        // Validate numerical values
        for row in matrix {
            for value in row {
                if value.isNaN || value.isInfinite {
                    throw APIError(message: "Transform matrix contains invalid numbers")
                }
            }
        }

        // Check for degenerate matrix
        let simdMatrix = convertToSIMDMatrix(matrix, req: req)
        let determinant = abs(simdMatrix.determinant)
        guard determinant > 1e-6 else {
            throw APIError(message: "Degenerate transform matrix (near-zero determinant)")
        }

        return matrix
    }

    private func embedPoseData(in sourceURL: URL, to destinationURL: URL, with transform: simd_float4x4, poseJson: [String: Any]?, req: Request) throws {
        guard let source = CGImageSourceCreateWithURL(sourceURL as CFURL, nil) else {
            throw APIError(message: "Failed to create image source for \(sourceURL.lastPathComponent)")
        }

        var imageData = try Data(contentsOf: sourceURL)

        // Resize image if too large
        if let cgImage = CGImageSourceCreateImageAtIndex(source, 0, nil) {
            let originalWidth = cgImage.width
            let originalHeight = cgImage.height
            let maxDimension = 1920

            if originalWidth > maxDimension || originalHeight > maxDimension {
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
                        req.logger.debug("Resized image from \(originalWidth)x\(originalHeight) to \(newWidth)x\(newHeight)")
                    }
                }
            }
        }

        guard let destination = CGImageDestinationCreateWithURL(destinationURL as CFURL, UTType.jpeg.identifier as CFString, 1, nil) else {
            throw APIError(message: "Failed to create image destination for \(destinationURL.lastPathComponent)")
        }

        // Prepare metadata
        var metadata = CGImageSourceCopyPropertiesAtIndex(source, 0, nil) as? [String: Any] ?? [:]
        var exifDict = metadata[kCGImagePropertyExifDictionary as String] as? [String: Any] ?? [:]

        // Add transform data
        let transformDict: [String: [Float]] = [
            "row0": [transform.columns.0.x, transform.columns.0.y, transform.columns.0.z, transform.columns.0.w],
            "row1": [transform.columns.1.x, transform.columns.1.y, transform.columns.1.z, transform.columns.1.w],
            "row2": [transform.columns.2.x, transform.columns.2.y, transform.columns.2.z, transform.columns.2.w],
            "row3": [transform.columns.3.x, transform.columns.3.y, transform.columns.3.z, transform.columns.3.w]
        ]
        exifDict["CameraTransform"] = transformDict

        // Add ARKit metadata if available
        if let poseJson = poseJson {
            exifDict["ARKitMetadata"] = [
                "confidence": poseJson["confidence"] as? Double ?? 0.0,
                "tracking_state": poseJson["tracking_state"] as? String ?? "Unknown",
                "feature_count": poseJson["feature_count"] as? Int ?? 0,
                "timestamp": poseJson["timestamp"] as? Double ?? 0.0,
                "light_estimate": poseJson["light_estimate"] as? Double ?? 0.0
            ]
        }

        metadata[kCGImagePropertyExifDictionary as String] = exifDict

        // Save the image with embedded metadata
        if let source = CGImageSourceCreateWithData(imageData as CFData, nil) {
            CGImageDestinationAddImageFromSource(destination, source, 0, metadata as CFDictionary)
            guard CGImageDestinationFinalize(destination) else {
                throw APIError(message: "Failed to save image with embedded pose data")
            }
            req.logger.debug("Successfully saved image with pose data: \(destinationURL.lastPathComponent)")
        }
    }

    private func createPLYMetadataFile(in folder: URL, req: Request) throws {
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
        let metadataURL = folder.appendingPathComponent("model.ply.json")
        try metadata.write(to: metadataURL, atomically: true, encoding: .utf8)
        req.logger.debug("Created PLY metadata file at \(metadataURL.path)")
    }

    private func convertToSIMDMatrix(_ matrix: [[Double]], req: Request) -> simd_float4x4 {
        return simd_float4x4(
            SIMD4<Float>(Float(matrix[0][0]), Float(matrix[0][1]), Float(matrix[0][2]), Float(matrix[0][3])),
            SIMD4<Float>(Float(matrix[1][0]), Float(matrix[1][1]), Float(matrix[1][2]), Float(matrix[1][3])),
            SIMD4<Float>(Float(matrix[2][0]), Float(matrix[2][1]), Float(matrix[2][2]), Float(matrix[2][3])),
            SIMD4<Float>(Float(matrix[3][0]), Float(matrix[3][1]), Float(matrix[3][2]), Float(matrix[3][3]))
        )
    }

    private func getImagesFolder(from folderURL: URL, req: Request) -> URL? {
        let fileManager = FileManager.default
        var queue = [folderURL]
        var checkedPaths = Set<URL>()

        req.logger.debug("Searching for images folder in: \(folderURL.path)")
        let rootContents = (try? fileManager.contentsOfDirectory(at: folderURL, includingPropertiesForKeys: nil).map { $0.lastPathComponent }) ?? []
        req.logger.debug("Root contents: \(rootContents.joined(separator: ", "))")

        while !queue.isEmpty {
            let currentURL = queue.removeFirst()
            if checkedPaths.contains(currentURL) { continue }
            checkedPaths.insert(currentURL)

            guard fileManager.fileExists(atPath: currentURL.path) else {
                req.logger.debug("Skipping non-existent path: \(currentURL.path)")
                continue
            }

            do {
                let contents = try fileManager.contentsOfDirectory(at: currentURL, includingPropertiesForKeys: nil)
                let imageFiles = contents.filter {
                    let ext = $0.pathExtension.lowercased()
                    return (ext == "jpg" || ext == "jpeg") && !$0.lastPathComponent.lowercased().hasPrefix("debug_")
                }

                let validImageFiles = imageFiles.filter { imageURL in
                    let baseName = imageURL.deletingPathExtension().lastPathComponent
                    let poseURL = currentURL.appendingPathComponent("\(baseName)_pose.json")
                    let exists = fileManager.fileExists(atPath: poseURL.path)
                    if !exists {
                        req.logger.debug("No pose file for \(imageURL.lastPathComponent)")
                    }
                    return exists
                }

                req.logger.debug("Folder \(currentURL.path) has \(imageFiles.count) images, \(validImageFiles.count) with valid poses")
                if validImageFiles.count >= 10 {
                    req.logger.info("Found valid images folder with \(validImageFiles.count) images: \(currentURL.path)")
                    return currentURL
                }

                // Add subdirectories to queue
                let subdirs = contents.filter { $0.hasDirectoryPath }
                queue.append(contentsOf: subdirs)
            } catch {
                req.logger.error("Error accessing \(currentURL.path): \(error)")
                continue
            }
        }

        req.logger.error("No folder found with at least 10 valid images and pose files")
        return nil
    }

    private func getPLYFile(from folderURL: URL, req: Request) -> URL? {
        let plyURL = folderURL.appendingPathComponent("model.ply")
        guard FileManager.default.fileExists(atPath: plyURL.path) else {
            return nil
        }

        do {
            let attributes = try FileManager.default.attributesOfItem(atPath: plyURL.path)
            let size = attributes[.size] as? Int64 ?? 0
            return size > 1024 ? plyURL : nil
        } catch {
            req.logger.debug("Could not check PLY file size: \(error)")
            return nil
        }
    }

    private func validatePLYFile(_ plyURL: URL, state: inout ProcessingState, req: Request) throws -> Bool {
        do {
            // Check file size
            let attributes = try FileManager.default.attributesOfItem(atPath: plyURL.path)
            let fileSize = (attributes[.size] as? Int64) ?? 0
            guard fileSize > 1024 else {
                state.statusMessage = "PLY file too small (needs at least 1KB)"
                return false
            }

            // Read header
            let fileHandle = try FileHandle(forReadingFrom: plyURL)
            defer { fileHandle.closeFile() }
            let headerData = fileHandle.readData(ofLength: 1024)

            guard let header = String(data: headerData, encoding: .ascii) else {
                state.statusMessage = "PLY header contains invalid characters"
                return false
            }

            // Validate header format
            guard header.lowercased().hasPrefix("ply") else {
                state.statusMessage = "Invalid PLY file format"
                return false
            }

            // Check for vertex data
            guard header.contains("element vertex") else {
                state.statusMessage = "PLY file missing vertex data"
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

            guard vertexCount > 1000 else {
                state.statusMessage = "Insufficient vertices (found \(vertexCount), need ≥1000)"
                return false
            }

            // Check for required properties
            let hasPosition = header.contains("property float x") &&
                             header.contains("property float y") &&
                             header.contains("property float z")

            let hasNormals = header.contains("property float nx") &&
                            header.contains("property float ny") &&
                            header.contains("property float nz")

            if !hasPosition {
                state.statusMessage = "PLY missing position data (x,y,z properties)"
                return false
            }

            if !hasNormals {
                state.statusMessage = "PLY missing normals (may affect quality)"
            }

            return true
        } catch {
            state.statusMessage = "PLY validation error: \(sanitizeError(error: error.localizedDescription))"
            throw APIError(message: "PLY validation failed: \(error.localizedDescription)")
        }
    }

    private func cleanupTemporaryFolders(_ req: Request) {
        Task {
            let tempDir = FileManager.default.temporaryDirectory
            do {
                let contents = try FileManager.default.contentsOfDirectory(at: tempDir, includingPropertiesForKeys: nil)
                for item in contents where item.lastPathComponent.contains("photogrammetry_") {
                    try? FileManager.default.removeItem(at: item)
                }
                req.logger.info("Cleaned up temporary folders")
            } catch {
                req.logger.error("Temporary folder cleanup failed: \(error)")
            }
        }
    }

    private func sanitizeError(error: String) -> String {
        return error
            .replacingOccurrences(of: "The operation couldn't be completed. ", with: "")
            .replacingOccurrences(of: "(com.apple.photogrammetry ", with: "")
    }

    private func CGImageDestinationCreateDataWithOptions(image: CGImage, type: UTType, count: Int, options: [CFString: Any], req: Request) -> Data? {
        let data = NSMutableData()
        guard let destination = CGImageDestinationCreateWithData(data as CFMutableData, type.identifier as CFString, count, nil) else {
            return nil
        }
        CGImageDestinationAddImage(destination, image, options as CFDictionary)
        let success = CGImageDestinationFinalize(destination)
        return success ? data as Data : nil
    }
}

extension FileManager {
    func copyContentsOfDirectory(at srcURL: URL, to dstURL: URL) throws {
        try createDirectory(at: dstURL, withIntermediateDirectories: true)
        for item in try contentsOfDirectory(at: srcURL, includingPropertiesForKeys: nil) {
            try copyItem(at: item, to: dstURL.appendingPathComponent(item.lastPathComponent))
        }
    }
}

extension PhotogrammetrySession {
    func finish() {
        self.cancel()
    }
}
