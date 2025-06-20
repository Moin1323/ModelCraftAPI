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
    private let baseUrl = "http://localhost:8080"
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
        let publicModelsDir = URL(fileURLWithPath: req.application.directory.publicDirectory).appendingPathComponent("models")

        do {
            try fileManager.createDirectory(at: inputDir, withIntermediateDirectories: true)
            try fileManager.createDirectory(at: outputDir, withIntermediateDirectories: true)
            try fileManager.createDirectory(at: publicModelsDir, withIntermediateDirectories: true)
        } catch {
            req.logger.error("Failed to create directories: \(error)")
            let apiError = APIError(message: "Failed to create directories: \(error.localizedDescription)")
            let data = try JSONEncoder().encode(apiError)
            return Response(status: .internalServerError, body: .init(data: data))
        }
        
        let zipURL = inputDir.appendingPathComponent(file.filename.lowercased().hasSuffix(".zip") ? file.filename : "\(file.filename).zip")
        try await req.fileio.writeFile(file.data, at: zipURL.path)
        req.logger.debug("Saved uploaded ZIP to: \(zipURL.path)")
        
        do {
            try extractZip(zipURL, to: inputDir, req: req)
            
            let state = ProcessingState(selectedFolderURL: inputDir)
            await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)

            let tempOutputFileURL = outputDir.appendingPathComponent("output_model.usdz")
            let response = try await startProcessing(outputURL: tempOutputFileURL, req: req, state: state)
            
            let tempDirPath = tempDir.path
            Task.detached { @Sendable in
                do {
                    try FileManager.default.removeItem(atPath: tempDirPath)
                    req.logger.info("Successfully cleaned up temporary directory: \(tempDirPath)")
                } catch {
                    req.logger.error("Background cleanup failed: \(error)")
                }
            }
            req.logger.info("Returning response: \(response)")
            let data = try JSONEncoder().encode(response)
            return Response(status: .ok, body: .init(data: data))
        } catch {
            do {
                try fileManager.removeItem(at: tempDir)
            } catch {
                req.logger.error("Cleanup failed: \(error)")
            }
            req.logger.error("Processing failed: \(error)")
            let errorMessage = error.localizedDescription.contains("Missing images folder") ? error.localizedDescription : "Processing failed: \(error.localizedDescription)"
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
        req.logger.debug("Trying to get uploaded file")
        
        // Log raw request body for debugging
        if let bodyData = req.body.data {
            let debugRawPath = "/tmp/uploaded_raw_body.bin"
            do {
                try await req.fileio.writeFile(bodyData, at: debugRawPath)
                req.logger.debug("Saved raw request body to: \(debugRawPath), size: \(bodyData.readableBytes)")
            } catch {
                req.logger.error("Failed to save raw body: \(error)")
            }
        }
        
        // Try multipart form-data with 'file' field
        if let formFile = try? req.content.decode(FileUpload.self) {
            req.logger.debug("Got file from multipart form at 'file' field, filename: \(formFile.filename), size: \(formFile.data.readableBytes)")
            let debugPath = "/tmp/uploaded_multipart.zip"
            do {
                let data = Data(buffer: formFile.data)
                try data.write(to: URL(fileURLWithPath: debugPath))
                req.logger.debug("Saved multipart file to: \(debugPath)")
                let archive = try Archive(url: URL(fileURLWithPath: debugPath), accessMode: .read)
                let contents = archive.map { $0.path }.joined(separator: ", ")
                req.logger.debug("Multipart ZIP contents: \(contents)")
            } catch {
                req.logger.error("Failed to save or read multipart debug file: \(error)")
            }
            return File(data: formFile.data, filename: formFile.filename)
        }
        // Try multipart form-data with 'data' field
        else if let formFile = try? req.content.decode(FileUpload.self) {
            req.logger.debug("Got file from multipart form at 'data' field, filename: \(formFile.filename), size: \(formFile.data.readableBytes)")
            let debugPath = "/tmp/uploaded_multipart_data.zip"
            do {
                let data = Data(buffer: formFile.data)
                try data.write(to: URL(fileURLWithPath: debugPath))
                req.logger.debug("Saved multipart file to: \(debugPath)")
                let archive = try Archive(url: URL(fileURLWithPath: debugPath), accessMode: .read)
                let contents = archive.map { $0.path }.joined(separator: ", ")
                req.logger.debug("Multipart ZIP contents: \(contents)")
            } catch {
                req.logger.error("Failed to save or read multipart debug file: \(error)")
            }
            return File(data: formFile.data, filename: formFile.filename)
        }
        // Try direct file upload
        else if let uploadedFile = try? req.content.decode(File.self) {
            req.logger.debug("Got direct file upload, filename: \(uploadedFile.filename), size: \(uploadedFile.data.readableBytes)")
            return uploadedFile
        }
        // Fallback to raw body
        else if let bodyData = req.body.data {
            let filename = req.headers.contentDisposition?.filename ?? req.headers.first(name: "filename") ?? "uploaded_file.zip"
            req.logger.debug("Got file from raw body data, filename: \(filename), size: \(bodyData.readableBytes)")
            let debugPath = "/tmp/uploaded_raw.zip"
            do {
                try await req.fileio.writeFile(bodyData, at: debugPath)
                req.logger.debug("Saved raw file to: \(debugPath)")
                let archive = try Archive(url: URL(fileURLWithPath: debugPath), accessMode: .read)
                let contents = archive.map { $0.path }.joined(separator: ", ")
                req.logger.debug("Raw ZIP contents: \(contents)")
            } catch {
                req.logger.error("Failed to save or read raw debug file: \(error)")
            }
            return File(data: bodyData, filename: filename)
        }
        
        req.logger.error("Failed to get file from any method")
        throw APIError(message: "No file uploaded or invalid field name")
    }
    
    private func extractZip(_ zipURL: URL, to destination: URL, req: Request, depth: Int = 0) throws {
        guard depth < 3 else { throw APIError(message: "Too many nested ZIP files") }
        
        let fileManager = FileManager.default
        req.logger.notice("Extracting ZIP: \(zipURL.path) to \(destination.path)")
        
        do {
            let archive = try Archive(url: zipURL, accessMode: .read)
            req.logger.debug("ZIP contents:")
            for entry in archive {
                req.logger.debug("- \(entry.path)")
            }
            
            try fileManager.unzipItem(at: zipURL, to: destination)
            
            let contents = try fileManager.contentsOfDirectory(at: destination, includingPropertiesForKeys: nil)
            req.logger.notice("Extracted \(contents.count) items:")
            for item in contents {
                req.logger.debug("- \(item.lastPathComponent)")
            }
            
            // Check for nested ZIPs
            for item in contents where item.pathExtension.lowercased() == "zip" && item.lastPathComponent != zipURL.lastPathComponent {
                let nestedDest = destination.appendingPathComponent(item.deletingPathExtension().lastPathComponent)
                try fileManager.createDirectory(at: nestedDest, withIntermediateDirectories: true)
                try extractZip(item, to: nestedDest, req: req, depth: depth + 1)
                try fileManager.removeItem(at: item)
            }
        } catch {
            req.logger.error("ZIP extraction failed: \(error)")
            throw APIError(message: "Failed to extract ZIP: \(error.localizedDescription)")
        }
    }

    private func startProcessing(outputURL: URL, req: Request, state: ProcessingState) async throws -> ModelResponse {
        var state = state
        guard let folderURL = state.selectedFolderURL else {
            throw APIError(message: "No folder selected")
        }

        state.isProcessing = true
        state.progress = 0.0
        state.statusMessage = "Processing started"
        state.processingStage = "Preparing data"
        await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)

        let usePLYAsReference = try await shouldUsePLYReference(from: folderURL, req: req, state: &state)
        let (inputFolder, imagesFolder) = try prepareInputFolder(folderURL, usePLY: usePLYAsReference, req: req)

        var configuration = PhotogrammetrySession.Configuration()
        configuration.sampleOrdering = .sequential
        configuration.featureSensitivity = usePLYAsReference ? .high : .normal
        configuration.isObjectMaskingEnabled = false

        let session = try PhotogrammetrySession(input: imagesFolder, configuration: configuration)
        state.photogrammetrySession = session
        await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)

        let timeoutTask = Task {
            try await Task.sleep(nanoseconds: 1_200_000_000_000)
            session.finish()
            throw APIError(message: "Processing timed out after 1200 seconds")
        }

        do {
            try session.process(requests: [.modelFile(url: outputURL, detail: optimalDetailLevel)])
            var response: ModelResponse?
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
            timeoutTask.cancel()
            session.finish()
            guard let response = response else {
                throw APIError(message: "Failed to generate response")
            }
            return response
        } catch {
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
                    try FileManager.default.moveItem(at: url, to: outputURL)
                    let publicModelsDir = URL(fileURLWithPath: req.application.directory.publicDirectory).appendingPathComponent("models")
                    let outputFileName = "output_\(UUID().uuidString).usdz"
                    let finalOutputFileURL = publicModelsDir.appendingPathComponent(outputFileName)
                    try FileManager.default.moveItem(at: outputURL, to: finalOutputFileURL)
                    req.logger.info("Saved USDZ model to: \(finalOutputFileURL.path)")

                    let modelUrl = "\(baseUrl)/models/\(outputFileName)"
                    let expirationDate = Date().addingTimeInterval(retentionPeriod)
                    let dateFormatter = ISO8601DateFormatter()
                    dateFormatter.timeZone = TimeZone(identifier: "Asia/Karachi")
                    let expiresAt = dateFormatter.string(from: expirationDate)

                    response = ModelResponse(
                        modelUrl: modelUrl,
                        statusMessage: "Model processed and saved successfully",
                        expiresAt: expiresAt
                    )
                    req.logger.info("Returning response: \(response.map { "\($0.modelUrl), \($0.statusMessage)" } ?? "nil")")
                    newState.statusMessage = "Model saved"
                    newState.isProcessing = false
                } catch {
                    req.logger.error("File move failed: \(error)")
                    newState.statusMessage = "Error: Failed to save model"
                    newState.isProcessing = false
                    throw APIError(message: "Failed to save model: \(error.localizedDescription)")
                }
            }

        case .requestError(_, let error):
            newState.statusMessage = "Error: \(sanitizeError(error: error.localizedDescription))"
            newState.isProcessing = false
            req.logger.error("Processing error: \(error.localizedDescription)")

        case .invalidSample(let id, let reason):
            req.logger.warning("Invalid sample ID \(id): \(reason)")

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
        }
    }

    private func cleanup(tempFolder: URL, logger: Logger, req: Request) async throws {
        do {
            try FileManager.default.removeItem(at: tempFolder)
            if var state = req.storage[ProcessingStateKey.self] {
                state.isProcessing = false
                state.photogrammetrySession = nil
                await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
            }
        } catch {
            logger.error("Cleanup failed: \(error)")
            throw APIError(message: "Cleanup failed: \(error.localizedDescription)")
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

        guard let imagesFolder = getImagesFolder(from: folderURL, req: req) else {
            throw APIError(message: "Missing images folder. Ensure the ZIP contains an 'images' folder with at least 10 JPEG images and corresponding pose files.")
        }

        let processedImages = try processImagesWithPoses(from: imagesFolder, req: req)
        let imagesDestFolder = tempFolder.appendingPathComponent("images")
        try FileManager.default.createDirectory(at: imagesDestFolder, withIntermediateDirectories: true)
        try FileManager.default.copyContentsOfDirectory(at: processedImages, to: imagesDestFolder)

        if usePLY, let plyFile = getPLYFile(from: folderURL, req: req) {
            let plyDest = tempFolder.appendingPathComponent("model.ply")
            try FileManager.default.copyItem(at: plyFile, to: plyDest)
            try createPLYMetadataFile(in: tempFolder, req: req)
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
                continue
            }
            
            do {
                let poseData = try Data(contentsOf: poseURL)
                let poseJson = try JSONSerialization.jsonObject(with: poseData) as? [String: Any]
                
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
                
                let poseMatrix = try validatePoseJSON(poseData, req: req)
                let transform = convertToSIMDMatrix(poseMatrix, req: req)
                let outputURL = tempFolder.appendingPathComponent(imageURL.lastPathComponent)
                try embedPoseData(in: imageURL, to: outputURL, with: transform, poseJson: poseJson, req: req)
                processedCount += 1
            } catch {
                continue
            }
        }
        
        guard processedCount >= 10 else {
            throw APIError(message: """
                Need at least 10 valid images (found \(processedCount)). 
                \(skippedDueToQuality > 0 ? "\(skippedDueToQuality) were skipped due to low quality." : "")
                """)
        }
        
        return tempFolder
    }

    private func validatePoseJSON(_ data: Data, req: Request) throws -> [[Double]] {
        guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let matrix = json["transform"] as? [[Double]] else {
            throw APIError(message: "Missing or invalid transform matrix")
        }
        
        guard matrix.count == 4, matrix.allSatisfy({ $0.count == 4 }) else {
            throw APIError(message: "Matrix must be 4x4")
        }
        
        for row in matrix {
            for value in row {
                if value.isNaN || value.isInfinite {
                    throw APIError(message: "Matrix contains invalid numbers")
                }
            }
        }
        
        let simdMatrix = convertToSIMDMatrix(matrix, req: req)
        let determinant = abs(simdMatrix.determinant)
        guard determinant > 1e-6 else {
            throw APIError(message: "Degenerate transform matrix")
        }
        
        return matrix
    }

    private func embedPoseData(in sourceURL: URL, to destinationURL: URL, with transform: simd_float4x4, poseJson: [String: Any]?, req: Request) throws {
        guard let source = CGImageSourceCreateWithURL(sourceURL as CFURL, nil) else {
            throw APIError(message: "Failed to create image source")
        }

        var imageData = try Data(contentsOf: sourceURL)
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
                    }
                }
            }
        }

        guard let destination = CGImageDestinationCreateWithURL(destinationURL as CFURL, UTType.jpeg.identifier as CFString, 1, nil) else {
            throw APIError(message: "Failed to create image destination")
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
        }

        metadata[kCGImagePropertyExifDictionary as String] = exifDict

        if let source = CGImageSourceCreateWithData(imageData as CFData, nil) {
            CGImageDestinationAddImageFromSource(destination, source, 0, metadata as CFDictionary)
            guard CGImageDestinationFinalize(destination) else {
                throw APIError(message: "Failed to save image with pose data")
            }
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
        try metadata.write(to: folder.appendingPathComponent("model.ply.json"), atomically: true, encoding: .utf8)
    }

    private func convertToSIMDMatrix(_ matrix: [[Double]], req: Request) -> simd_float4x4 {
        simd_float4x4(
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
        req.logger.debug("ZIP root contents: \(rootContents.joined(separator: ", "))")

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
                
                req.logger.debug("Folder \(currentURL.path) has \(imageFiles.count) JPEGs, \(validImageFiles.count) with pose files")
                if validImageFiles.count >= 10 {
                    req.logger.info("Found valid images folder with \(validImageFiles.count) image-pose pairs: \(currentURL.path)")
                    return currentURL
                }
                
                let subdirs = contents.filter { $0.hasDirectoryPath }
                queue.append(contentsOf: subdirs)
            } catch {
                req.logger.error("Error accessing \(currentURL.path): \(error)")
                continue
            }
        }
        
        req.logger.error("No folder found with ≥10 JPEG images and corresponding pose files")
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
            return nil
        }
    }

    private func validatePLYFile(_ plyURL: URL, state: inout ProcessingState, req: Request) throws -> Bool {
        do {
            let attributes = try FileManager.default.attributesOfItem(atPath: plyURL.path)
            let fileSize = (attributes[.size] as? Int64) ?? 0
            guard fileSize > 1024 else {
                state.statusMessage = "PLY file too small (needs at least 1KB)"
                return false
            }
            
            let fileHandle = try FileHandle(forReadingFrom: plyURL)
            defer { fileHandle.closeFile() }
            let headerData = fileHandle.readData(ofLength: 1024)
            
            guard let header = String(data: headerData, encoding: .ascii) else {
                state.statusMessage = "PLY header contains invalid characters"
                return false
            }
            
            guard header.lowercased().hasPrefix("ply") else {
                state.statusMessage = "Invalid PLY file (missing header)"
                return false
            }
            
            guard header.contains("element vertex") else {
                state.statusMessage = "PLY file missing vertex data"
                return false
            }
            
            var vertexCount = 0
            if let vertexRange = header.range(of: "element vertex") {
                let remainingHeader = header[vertexRange.upperBound...]
                if let countRange = remainingHeader.range(of: "\\d+", options: .regularExpression) {
                    vertexCount = Int(remainingHeader[countRange]) ?? 0
                }
            }
            
            guard vertexCount > 1000 else {
                state.statusMessage = "PLY needs ≥1000 vertices (found: \(vertexCount))"
                return false
            }
            
            let hasPosition = header.contains("property float x") &&
                             header.contains("property float y") &&
                             header.contains("property float z")
            
            let hasNormals = header.contains("property float nx") &&
                            header.contains("property float ny") &&
                            header.contains("property float nz")
            
            if !hasPosition {
                state.statusMessage = "PLY missing position data"
                return false
            }
            
            if !hasNormals {
                state.statusMessage = "PLY missing normals (may affect quality)"
            }
            
            return true
        } catch {
            state.statusMessage = "Error during PLY validation: \(sanitizeError(error: error.localizedDescription))"
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
            } catch {
                req.logger.error("Cleanup error: \(error)")
            }
        }
    }

    private func sanitizeError(error: String) -> String {
        error
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
