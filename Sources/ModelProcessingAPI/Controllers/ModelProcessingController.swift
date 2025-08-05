import Vapor
@preconcurrency import RealityKit
import UniformTypeIdentifiers
import Foundation
import CoreGraphics
import ImageIO
import ZIPFoundation
import NIO

// MARK: - Error Handling System

enum ProcessingError: Error, CustomStringConvertible {
    // Input Validation Errors (400 range)
    case invalidRequest(reason: String)
    case fileTooLarge(maxSize: Int)
    case invalidFileType
    case insufficientImages(minRequired: Int, found: Int)
    case invalidImageData
    case invalidPoseData
    case invalidPLYFile(reason: String)
    
    // Processing Errors (500 range)
    case processingFailed(reason: String)
    case timeoutElapsed
    case sessionError(code: Int)
    case systemResourcesUnavailable
    case outputGenerationFailed
    
    // File System Errors
    case fileSystemError(reason: String)
    case directoryCreationFailed
    case cleanupFailed
    
    // Concurrency Errors
    case tooManyConcurrentTasks(max: Int)
    
    var description: String {
        switch self {
        case .invalidRequest(let reason):
            return "Invalid request: \(reason)"
        case .fileTooLarge(let maxSize):
            let sizeInMB = maxSize / 1_000_000
            return "File exceeds maximum size of \(sizeInMB)MB"
        case .invalidFileType:
            return "Unsupported file type. Please upload a ZIP archive containing images"
        case .insufficientImages(let minRequired, let found):
            return "Not enough valid images (minimum \(minRequired), found \(found))"
        case .invalidImageData:
            return "One or more images couldn't be processed"
        case .invalidPoseData:
            return "Missing or invalid pose data for images"
        case .invalidPLYFile(let reason):
            return "Invalid 3D reference file: \(reason)"
            
        case .processingFailed(let reason):
            return "Model processing failed: \(reason)"
        case .timeoutElapsed:
            return "Processing took too long and was cancelled"
        case .sessionError(let code):
            return "Processing error (code: \(code)). Please try again with different images"
        case .systemResourcesUnavailable:
            return "System resources unavailable. Please try again later"
        case .outputGenerationFailed:
            return "Could not generate 3D model from the provided images"
            
        case .fileSystemError(let reason):
            return "File system error: \(reason)"
        case .directoryCreationFailed:
            return "Could not create required directories"
        case .cleanupFailed:
            return "Temporary files cleanup failed"
            
        case .tooManyConcurrentTasks(let max):
            return "Server busy (processing \(max) models). Please try again shortly"
        }
    }
    
    var httpStatus: HTTPStatus {
        switch self {
        case .invalidRequest, .fileTooLarge, .invalidFileType,
             .insufficientImages, .invalidImageData, .invalidPoseData,
             .invalidPLYFile:
            return .badRequest
        case .tooManyConcurrentTasks:
            return .tooManyRequests
        default:
            return .internalServerError
        }
    }
}

// MARK: - Response Structures

struct ErrorResponse: Content {
    let error: Bool
    let reason: String
    let suggestion: String?
    let code: Int
    
    init(from error: ProcessingError) {
        self.error = true
        self.reason = error.description
        self.code = Int(error.httpStatus.code)
        self.suggestion = {
            switch error {
            case .insufficientImages:
                return "Ensure your ZIP contains at least 10 high-quality images with matching pose files"
            case .invalidPoseData:
                return "Check that each image has a corresponding _pose.json file with valid camera data"
            case .invalidFileType:
                return "Upload a ZIP file containing JPEG images in a folder named 'images'"
            case .processingFailed:
                return "Try again with different images or check image quality"
            case .fileTooLarge:
                return "Compress your images or split into smaller batches"
            case .tooManyConcurrentTasks:
                return "Wait a few minutes and try your request again"
            default:
                return nil
            }
        }()
    }
}

struct ProgressResponse: Content {
    let status: String
    let progress: Double
    let stage: String
    let isProcessing: Bool
}

struct ModelResponse: Content {
    let modelUrl: String
    let status: String
    let expiresAt: String
    let fileSize: Int
    let estimatedPolygons: Int?
}

// MARK: - Storage Keys

private struct ProcessingStateKey: StorageKey {
    typealias Value = ProcessingState
}

private struct ActiveTasksKey: StorageKey {
    typealias Value = Int
}

// MARK: - Request-Scoped State

private struct ProcessingState {
    var inputFolder: URL?
    var outputFile: URL?
    var status: String = "Waiting to start"
    var progress: Double = 0.0
    var stage: String = "Initializing"
    var isProcessing: Bool = false
    var session: PhotogrammetrySession?
    var startTime: Date = Date()
}

// MARK: - File Upload Struct

struct FileUpload: Content {
    var data: ByteBuffer
    var filename: String
}

@MainActor
final class ModelProcessingController {
    private let optimalDetailLevel: PhotogrammetrySession.Request.Detail = .medium
    private let minPoseConfidence: Double = 0.3
    private let minFeatureCount: Int = 10
    private let maxFileSize: Int = 600_000_000
    private let maxConcurrentTasks: Int = 4
    private let baseUrl = "http://213.73.97.120"
    private let retentionPeriod: TimeInterval = 24 * 3600
    private let processingTimeout: TimeInterval = 1200 // 20 minutes

    func processModel(_ req: Request) async throws -> Response {
        try checkConcurrencyLimit(req)
        
        let file = try await validateAndExtractUpload(req: req)
        let directories = try createProcessingDirectories(req: req)
        
        return try await processUploadedFile(
            file: file,
            directories: directories,
            req: req
        )
    }

    func getProgress(_ req: Request) async throws -> ProgressResponse {
        guard let state = req.storage[ProcessingStateKey.self] else {
            throw ProcessingError.invalidRequest(reason: "No active processing session")
        }
        
        return ProgressResponse(
            status: state.status,
            progress: state.progress,
            stage: state.stage,
            isProcessing: state.isProcessing
        )
    }

    // MARK: - Private Implementation

    private func checkConcurrencyLimit(_ req: Request) throws {
        let activeTasks = req.application.storage[ActiveTasksKey.self] ?? 0
        // guard activeTasks < maxConcurrentTasks else {
        //     throw ProcessingError.tooManyConcurrentTasks(max: maxConcurrentTasks)
        // }
        req.application.storage[ActiveTasksKey.self] = activeTasks + 1
        req.logger.info("Incremented active tasks to \(activeTasks + 1)")
    }

    private func validateAndExtractUpload(req: Request) async throws -> File {
        let file: File
        
        do {
            file = try await getUploadedFile(req: req)
        } catch {
            req.logger.error("File upload error: \(error)")
            throw ProcessingError.invalidRequest(reason: "Invalid file upload. Please use 'file' field or raw body.")
        }
        
        guard file.data.readableBytes < maxFileSize else {
            throw ProcessingError.fileTooLarge(maxSize: maxFileSize)
        }
        
        guard file.filename.lowercased().hasSuffix(".zip") else {
            throw ProcessingError.invalidFileType
        }
        
        return file
    }

    private func createProcessingDirectories(req: Request) throws -> (input: URL, output: URL) {
        let fileManager = FileManager.default
        let tempDir = fileManager.temporaryDirectory
            .appendingPathComponent("model_processing_\(UUID().uuidString)")
        let inputDir = tempDir.appendingPathComponent("input")
        let outputDir = tempDir.appendingPathComponent("output")
        
        do {
            try fileManager.createDirectory(at: inputDir, withIntermediateDirectories: true)
            try fileManager.createDirectory(at: outputDir, withIntermediateDirectories: true)
            
            var isDir: ObjCBool = false
            guard fileManager.fileExists(atPath: inputDir.path, isDirectory: &isDir), isDir.boolValue else {
                throw ProcessingError.directoryCreationFailed
            }
            
            req.logger.info("Created processing directories at \(tempDir.path)")
            return (inputDir, outputDir)
        } catch {
            req.logger.error("Directory creation failed: \(error)")
            throw ProcessingError.fileSystemError(reason: "Could not create processing directories")
        }
    }

    private func processUploadedFile(
        file: File,
        directories: (input: URL, output: URL),
        req: Request
    ) async throws -> Response {
        let zipURL = directories.input.appendingPathComponent(file.filename)
        
        do {
            try await req.fileio.writeFile(file.data, at: zipURL.path)
            req.logger.debug("Saved uploaded file to \(zipURL.path)")
            
            try extractZip(zipURL, to: directories.input, req: req)
            
            let (inputFolder, imagesFolder) = try prepareInputFolder(at: directories.input, req: req)
            let outputFile = directories.output.appendingPathComponent("model.usdz")
            
            let initialState = ProcessingState(
                inputFolder: inputFolder,
                outputFile: outputFile,
                status: "Starting processing",
                isProcessing: true
            )
            await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: initialState)
            
            let response = try await startPhotogrammetryProcessing(
                input: imagesFolder,
                output: outputFile,
                req: req
            )
            
            scheduleCleanup(directory: directories.input, req: req)
            req.application.storage[ActiveTasksKey.self] = (req.application.storage[ActiveTasksKey.self] ?? 1) - 1
            req.logger.info("Decremented active tasks to \(req.application.storage[ActiveTasksKey.self] ?? 0)")
            
            let data = try JSONEncoder().encode(response)
            return Response(status: .ok, body: .init(data: data))
            
        } catch let error as ProcessingError {
            try await cleanupOnFailure(directory: directories.input, req: req)
            return try handleProcessingError(error, req: req)
            
        } catch {
            req.logger.error("Unexpected processing error: \(error)")
            try await cleanupOnFailure(directory: directories.input, req: req)
            return try handleProcessingError(
                .processingFailed(reason: error.localizedDescription),
                req: req
            )
        }
    }

    // MARK: - Photogrammetry Processing

    private func startPhotogrammetryProcessing(
        input: URL,
        output: URL,
        req: Request
    ) async throws -> ModelResponse {
        var configuration = PhotogrammetrySession.Configuration()
        configuration.sampleOrdering = .sequential
        configuration.featureSensitivity = .normal
        configuration.isObjectMaskingEnabled = false
        
        do {
            let session = try PhotogrammetrySession(input: input, configuration: configuration)
            
            if var state = req.storage[ProcessingStateKey.self] {
                state.session = session
                await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
            }
            
            let timeoutTask = Task {
                try await Task.sleep(for: .seconds(processingTimeout))
                session.cancel()
                throw ProcessingError.timeoutElapsed
            }
            
            defer {
                timeoutTask.cancel()
                session.cancel()
                if var state = req.storage[ProcessingStateKey.self] {
                    state.isProcessing = false
                    state.session = nil
                    req.storage[ProcessingStateKey.self] = state // Synchronous update
                }
            }
            
            try session.process(requests: [.modelFile(url: output, detail: optimalDetailLevel)])
            
            for try await outputResult in session.outputs {
                if case .requestComplete(_, let result) = outputResult,
                   case .modelFile(url: let url) = result {
                    let response = try handleCompletedModel(url: url, outputURL: output, req: req)
                    // Update state after successful completion
                    if var state = req.storage[ProcessingStateKey.self] {
                        state.status = "Processing complete"
                        state.progress = 1.0
                        state.isProcessing = false
                        state.session = nil
                        await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
                    }
                    return response
                }
                
                try await updateProcessingState(from: outputResult, req: req)
            }
            
            throw ProcessingError.outputGenerationFailed
        } catch let error as ProcessingError {
            // Update state before throwing
            if var state = req.storage[ProcessingStateKey.self] {
                state.isProcessing = false
                state.session = nil
                state.status = "Error: \(error.description)"
                await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
            }
            throw error
        } catch {
            // Update state before throwing
            if var state = req.storage[ProcessingStateKey.self] {
                state.isProcessing = false
                state.session = nil
                state.status = "Error: \(sanitizeError(error: error.localizedDescription))"
                await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
            }
            throw ProcessingError.processingFailed(reason: sanitizeError(error: error.localizedDescription))
        }
    }

    private func handleCompletedModel(url: URL, outputURL: URL, req: Request) throws -> ModelResponse {
        let fileManager = FileManager.default
        let publicModelsDir = URL(fileURLWithPath: "/tmp/models")
        
        try fileManager.createDirectory(at: publicModelsDir, withIntermediateDirectories: true)
        
        let outputFileName = "model_\(UUID().uuidString).usdz"
        let finalOutputURL = publicModelsDir.appendingPathComponent(outputFileName)
        
        try fileManager.moveItem(at: url, to: finalOutputURL)
        
        let attributes = try fileManager.attributesOfItem(atPath: finalOutputURL.path)
        let fileSize = (attributes[.size] as? Int) ?? 0
        
        let modelUrl = "\(baseUrl)/models/\(outputFileName)"
        let expirationDate = Date().addingTimeInterval(retentionPeriod)
        let dateFormatter = ISO8601DateFormatter()
        dateFormatter.timeZone = TimeZone(identifier: "Asia/Karachi")
        let expiresAt = dateFormatter.string(from: expirationDate)
        
        req.logger.info("Successfully generated model at \(finalOutputURL.path)")
        
        return ModelResponse(
            modelUrl: modelUrl,
            status: "Model generated successfully",
            expiresAt: expiresAt,
            fileSize: fileSize,
            estimatedPolygons: nil
        )
    }

    private func updateProcessingState(from output: PhotogrammetrySession.Output, req: Request) async throws {
        guard var state = req.storage[ProcessingStateKey.self] else {
            return
        }
        
        switch output {
        case .processingComplete:
            state.status = "Processing complete"
            state.progress = 1.0
            state.isProcessing = false
            
        case .requestProgress(_, let fraction):
            state.progress = fraction
            updateProcessingStage(fraction, state: &state, req: req)
            
        case .requestError(_, let error):
            state.status = "Error: \(sanitizeError(error: error.localizedDescription))"
            state.isProcessing = false
            throw ProcessingError.sessionError(code: (error as NSError).code)
            
        case .processingCancelled:
            state.status = "Processing cancelled"
            state.isProcessing = false
            throw ProcessingError.timeoutElapsed
            
        case .invalidSample(let id, let reason):
            req.logger.warning("Invalid sample (ID: \(id)): \(reason)")
            if reason.contains("No bounding box found") || reason.contains("missing LiDAR point cloud") {
                state.status = "Warning: Some images may lack sufficient data"
            }
            
        default:
            break
        }
        
        await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
    }

    private func updateProcessingStage(_ progress: Double, state: inout ProcessingState, req: Request) {
        let stage: String
        switch progress {
        case 0..<0.2: stage = "Processing images"
        case 0.2..<0.5: stage = "Aligning point cloud"
        case 0.5..<0.8: stage = "Generating geometry"
        case 0.8..<1.0: stage = "Finalizing model"
        default: stage = "Processing"
        }
        
        if state.stage != stage {
            state.stage = stage
            req.logger.info("Processing stage: \(stage) (\(Int(progress * 100))%)")
        }
    }

    // MARK: - File Preparation

    private func prepareInputFolder(at folderURL: URL, req: Request) throws -> (tempFolder: URL, imagesFolder: URL) {
        let tempFolder = FileManager.default.temporaryDirectory
            .appendingPathComponent("photogrammetry_input_\(UUID().uuidString)")
        
        try FileManager.default.createDirectory(at: tempFolder, withIntermediateDirectories: true)
        
        guard let imagesFolder = findImagesFolder(in: folderURL, req: req) else {
            throw ProcessingError.invalidRequest(
                reason: "No valid images folder found. Include an 'images' folder with JPEGs and pose files."
            )
        }
        
        let processedImages = try processImagesWithPoses(from: imagesFolder, req: req)
        let imagesDestFolder = tempFolder.appendingPathComponent("images")
        try FileManager.default.createDirectory(at: imagesDestFolder, withIntermediateDirectories: true)
        try FileManager.default.copyContentsOfDirectory(at: processedImages, to: imagesDestFolder)
        
        if let plyFile = findPLYFile(in: folderURL, req: req),
           try validatePLYFile(plyFile, req: req) {
            let plyDest = tempFolder.appendingPathComponent("reference.ply")
            try FileManager.default.copyItem(at: plyFile, to: plyDest)
            try createPLYMetadataFile(in: tempFolder, req: req)
        }
        
        return (tempFolder, imagesDestFolder)
    }

    private func processImagesWithPoses(from folderURL: URL, req: Request) throws -> URL {
        let tempFolder = FileManager.default.temporaryDirectory
            .appendingPathComponent("processed_images_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempFolder, withIntermediateDirectories: true)
        
        let contents = try FileManager.default.contentsOfDirectory(at: folderURL, includingPropertiesForKeys: nil)
        let imageFiles = contents.filter {
            let ext = $0.pathExtension.lowercased()
            return ext == "jpg" || ext == "jpeg" || ext == "png"
        }
        
        var processedCount = 0
        var skippedDueToQuality = 0
        
        for imageURL in imageFiles {
            let baseName = imageURL.deletingPathExtension().lastPathComponent
            let poseURL = folderURL.appendingPathComponent("\(baseName)_pose.json")
            
            guard FileManager.default.fileExists(atPath: poseURL.path) else {
                req.logger.debug("Skipping image without pose file: \(imageURL.lastPathComponent)")
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
                
                let poseMatrix = try validatePoseJSON(poseData, req: req)
                let transform = convertToSIMDMatrix(poseMatrix)
                let outputURL = tempFolder.appendingPathComponent(imageURL.lastPathComponent)
                try embedPoseData(in: imageURL, to: outputURL, with: transform, req: req)
                processedCount += 1
                
            } catch {
                req.logger.debug("Skipping image due to error: \(error.localizedDescription)")
                continue
            }
        }
        
        guard processedCount >= 10 else {
            throw ProcessingError.insufficientImages(
                minRequired: 10,
                found: processedCount
            )
        }
        
        req.logger.info("Processed \(processedCount) valid images (skipped \(skippedDueToQuality) low quality)")
        return tempFolder
    }

    // MARK: - File Handling Utilities

    private func findImagesFolder(in directory: URL, req: Request) -> URL? {
        let fileManager = FileManager.default
        var queue = [directory]
        var checkedPaths = Set<URL>()
        
        while !queue.isEmpty {
            let currentURL = queue.removeFirst()
            if checkedPaths.contains(currentURL) { continue }
            checkedPaths.insert(currentURL)
            
            guard fileManager.fileExists(atPath: currentURL.path) else {
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
                    return fileManager.fileExists(atPath: poseURL.path)
                }
                
                if validImageFiles.count >= 10 {
                    return currentURL
                }
                
                let subdirs = contents.filter { $0.hasDirectoryPath }
                queue.append(contentsOf: subdirs)
                
            } catch {
                continue
            }
        }
        
        return nil
    }

    private func findPLYFile(in directory: URL, req: Request) -> URL? {
        let plyURL = directory.appendingPathComponent("model.ply")
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

    private func validatePLYFile(_ plyURL: URL, req: Request) throws -> Bool {
        do {
            let attributes = try FileManager.default.attributesOfItem(atPath: plyURL.path)
            let fileSize = (attributes[.size] as? Int64) ?? 0
            guard fileSize > 1024 else {
                throw ProcessingError.invalidPLYFile(reason: "File too small (minimum 1KB required)")
            }
            
            let fileHandle = try FileHandle(forReadingFrom: plyURL)
            defer { fileHandle.closeFile() }
            let headerData = fileHandle.readData(ofLength: 1024)
            
            guard let header = String(data: headerData, encoding: .ascii) else {
                throw ProcessingError.invalidPLYFile(reason: "Invalid file header")
            }
            
            guard header.lowercased().hasPrefix("ply") else {
                throw ProcessingError.invalidPLYFile(reason: "Not a valid PLY file")
            }
            
            guard header.contains("element vertex") else {
                throw ProcessingError.invalidPLYFile(reason: "Missing vertex data")
            }
            
            var vertexCount = 0
            if let vertexRange = header.range(of: "element vertex") {
                let remainingHeader = header[vertexRange.upperBound...]
                if let countRange = remainingHeader.range(of: "\\d+", options: .regularExpression) {
                    vertexCount = Int(remainingHeader[countRange]) ?? 0
                }
            }
            
            guard vertexCount > 1000 else {
                throw ProcessingError.invalidPLYFile(reason: "Insufficient vertices (minimum 1000 required)")
            }
            
            return true
        } catch let error as ProcessingError {
            throw error
        } catch {
            throw ProcessingError.invalidPLYFile(reason: error.localizedDescription)
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
        let metadataURL = folder.appendingPathComponent("reference.ply.json")
        try metadata.write(to: metadataURL, atomically: true, encoding: .utf8)
    }

    private func validatePoseJSON(_ data: Data, req: Request) throws -> [[Double]] {
        guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let matrix = json["transform"] as? [[Double]] else {
            throw ProcessingError.invalidPoseData
        }
        
        guard matrix.count == 4, matrix.allSatisfy({ $0.count == 4 }) else {
            throw ProcessingError.invalidPoseData
        }
        
        for row in matrix {
            for value in row {
                if value.isNaN || value.isInfinite {
                    throw ProcessingError.invalidPoseData
                }
            }
        }
        
        return matrix
    }

    private func convertToSIMDMatrix(_ matrix: [[Double]]) -> simd_float4x4 {
        return simd_float4x4(
            SIMD4<Float>(Float(matrix[0][0]), Float(matrix[0][1]), Float(matrix[0][2]), Float(matrix[0][3])),
            SIMD4<Float>(Float(matrix[1][0]), Float(matrix[1][1]), Float(matrix[1][2]), Float(matrix[1][3])),
            SIMD4<Float>(Float(matrix[2][0]), Float(matrix[2][1]), Float(matrix[2][2]), Float(matrix[2][3])),
            SIMD4<Float>(Float(matrix[3][0]), Float(matrix[3][1]), Float(matrix[3][2]), Float(matrix[3][3]))
        )
    }

    private func embedPoseData(in sourceURL: URL, to destinationURL: URL, with transform: simd_float4x4, req: Request) throws {
        guard let source = CGImageSourceCreateWithURL(sourceURL as CFURL, nil) else {
            throw ProcessingError.invalidImageData
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
                       let resizedData = CGImageDestination.createDataWithOptions(
                        image: resizedImage,
                        type: .jpeg,
                        count: 1,
                        options: [kCGImageDestinationLossyCompressionQuality as CFString: 0.8]
                       ) {
                        imageData = resizedData
                    }
                }
            }
        }
        
        guard let destination = CGImageDestinationCreateWithURL(
            destinationURL as CFURL,
            UTType.jpeg.identifier as CFString,
            1,
            nil
        ) else {
            throw ProcessingError.invalidImageData
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
        metadata[kCGImagePropertyExifDictionary as String] = exifDict
        
        if let source = CGImageSourceCreateWithData(imageData as CFData, nil) {
            CGImageDestinationAddImageFromSource(destination, source, 0, metadata as CFDictionary)
            guard CGImageDestinationFinalize(destination) else {
                throw ProcessingError.invalidImageData
            }
        }
    }

    private func extractZip(_ zipURL: URL, to destination: URL, req: Request, depth: Int = 0) throws {
        guard depth < 3 else {
            throw ProcessingError.invalidRequest(reason: "ZIP file nesting too deep (maximum 3 levels allowed)")
        }
        
        req.logger.notice("Extracting ZIP archive: \(zipURL.lastPathComponent)")
        
        do {
            try FileManager.default.unzipItem(at: zipURL, to: destination)
            
            let contents = try FileManager.default.contentsOfDirectory(at: destination, includingPropertiesForKeys: nil)
            for item in contents where item.pathExtension.lowercased() == "zip" &&
                                      item.lastPathComponent != zipURL.lastPathComponent {
                let nestedDest = destination.appendingPathComponent(item.deletingPathExtension().lastPathComponent)
                try FileManager.default.createDirectory(at: nestedDest, withIntermediateDirectories: true)
                try extractZip(item, to: nestedDest, req: req, depth: depth + 1)
                try FileManager.default.removeItem(at: item)
            }
        } catch {
            throw ProcessingError.invalidRequest(reason: "Failed to extract ZIP: \(error.localizedDescription)")
        }
    }

    private func getUploadedFile(req: Request) async throws -> File {
        if let formFile = try? req.content.decode(FileUpload.self) {
            req.logger.debug("Found file in 'file' field: \(formFile.filename), size: \(formFile.data.readableBytes)")
            return File(data: formFile.data, filename: formFile.filename)
        } else if let uploadedFile = try? req.content.decode(File.self) {
            req.logger.debug("Found direct file upload: \(uploadedFile.filename), size: \(uploadedFile.data.readableBytes)")
            return uploadedFile
        } else if let bodyData = req.body.data {
            let filename = req.headers.contentDisposition?.filename ??
                         req.headers.first(name: "filename") ??
                         "upload_\(UUID().uuidString).zip"
            req.logger.debug("Using raw body as file: \(filename), size: \(bodyData.readableBytes)")
            return File(data: bodyData, filename: filename)
        }
        
        req.logger.error("Could not locate file in request")
        throw ProcessingError.invalidRequest(reason: "No file found in request")
    }

    // MARK: - Error Handling

    private func handleProcessingError(_ error: ProcessingError, req: Request) throws -> Response {
        req.logger.error("Processing error: \(error.description)")
        
        let errorResponse = ErrorResponse(from: error)
        let httpResponse = Response(status: error.httpStatus)
        
        do {
            let data = try JSONEncoder().encode(errorResponse)
            httpResponse.body = .init(data: data)
            httpResponse.headers.replaceOrAdd(name: .contentType, value: "application/json")
        } catch {
            httpResponse.status = .internalServerError
            httpResponse.body = .init(string: "Failed to encode error response")
        }
        
        return httpResponse
    }

    // MARK: - Cleanup

    private func scheduleCleanup(directory: URL, req: Request) {
        Task.detached { @Sendable in
            do {
                try FileManager.default.removeItem(at: directory)
                req.logger.info("Cleaned up temporary directory: \(directory.path)")
            } catch {
                req.logger.error("Cleanup failed for \(directory.path): \(error)")
            }
        }
    }

    private func cleanupOnFailure(directory: URL, req: Request) async throws {
        do {
            try FileManager.default.removeItem(at: directory)
            req.logger.info("Cleaned up after failure: \(directory.path)")
            if var state = req.storage[ProcessingStateKey.self] {
                state.isProcessing = false
                state.session = nil
                await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)
            }
        } catch {
            req.logger.error("Failed to clean up after error: \(error)")
            throw ProcessingError.cleanupFailed
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

extension CGImageDestination {
    static func createDataWithOptions(
        image: CGImage,
        type: UTType,
        count: Int,
        options: [CFString: Any]
    ) -> Data? {
        let data = NSMutableData()
        guard let destination = CGImageDestinationCreateWithData(
            data as CFMutableData,
            type.identifier as CFString,
            count,
            nil
        ) else {
            return nil
        }
        CGImageDestinationAddImage(destination, image, options as CFDictionary)
        let success = CGImageDestinationFinalize(destination)
        if success {
            return Data(data)
        }
        return nil
    }
}
