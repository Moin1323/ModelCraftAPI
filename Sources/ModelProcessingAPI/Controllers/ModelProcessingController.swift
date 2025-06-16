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

    func processModel(_ req: Request) async throws -> Response {
        let activeTasks = req.application.storage[ActiveTasksKey.self] ?? 0
        guard activeTasks < maxConcurrentTasks else {
            throw Abort(.tooManyRequests, reason: "Server is busy, please try again later")
        }
        req.application.storage[ActiveTasksKey.self] = activeTasks + 1
        defer { req.application.storage[ActiveTasksKey.self] = max(0, activeTasks - 1) }

        let file: File = try await getUploadedFile(req: req)
        
        guard file.data.readableBytes < maxFileSize else {
            throw Abort(.payloadTooLarge, reason: "Uploaded file exceeds 600MB limit")
        }

        let fileManager = FileManager.default
        let tempDir = fileManager.temporaryDirectory.appendingPathComponent("model_processing_\(UUID().uuidString)")
        let inputDir = tempDir.appendingPathComponent("input")
        let outputDir = tempDir.appendingPathComponent("output")

        do {
            try fileManager.createDirectory(at: inputDir, withIntermediateDirectories: true)
            try fileManager.createDirectory(at: outputDir, withIntermediateDirectories: true)
            
            let zipURL = inputDir.appendingPathComponent(file.filename.lowercased().hasSuffix(".zip") ? file.filename : "\(file.filename).zip")
            try await req.fileio.writeFile(file.data, at: zipURL.path)
            
            try extractZip(zipURL, to: inputDir, req: req)
            
            var state = ProcessingState(selectedFolderURL: inputDir)
            await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)

            let outputFileURL = outputDir.appendingPathComponent("output_model.usdz")
            state = try await startProcessing(outputURL: outputFileURL, req: req, state: state)

            guard fileManager.fileExists(atPath: outputFileURL.path) else {
                throw Abort(.internalServerError, reason: "Failed to generate output model")
            }

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
            try? fileManager.removeItem(at: tempDir)
            throw Abort(.internalServerError, reason: "Processing failed: \(error.localizedDescription)")
        }
    }

    func getProgress(_ req: Request) async throws -> ProgressResponse {
        guard let state = req.storage[ProcessingStateKey.self] else {
            throw Abort(.notFound, reason: "No active processing session")
        }
        return ProgressResponse(
            statusMessage: state.statusMessage,
            progress: state.progress,
            processingStage: state.processingStage,
            isProcessing: state.isProcessing
        )
    }

    // MARK: - Private Methods
    
    private func getUploadedFile(req: Request) async throws -> File {
        if let formFile = try? req.content.get(FileUpload.self, at: "file") {
            return File(data: formFile.data, filename: formFile.filename)
        } else if let formFile = try? req.content.get(FileUpload.self, at: "data") {
            return File(data: formFile.data, filename: formFile.filename)
        } else if let uploadedFile = try? req.content.decode(File.self) {
            return uploadedFile
        } else if let bodyData = req.body.data {
            let filename = req.headers.contentDisposition?.filename ?? req.headers.first(name: "filename") ?? "uploaded_file.zip"
            return File(data: bodyData, filename: filename)
        }
        throw Abort(.badRequest, reason: "No file uploaded or invalid field name")
    }

    private func extractZip(_ zipURL: URL, to destination: URL, req: Request, depth: Int = 0) throws {
        guard depth < 5 else { throw Abort(.badRequest, reason: "Too many nested ZIP files") }
        
        let fileManager = FileManager.default
        let archive = try Archive(url: zipURL, accessMode: .read)
        try fileManager.unzipItem(at: zipURL, to: destination)
        
        for item in try fileManager.contentsOfDirectory(at: destination, includingPropertiesForKeys: nil)
        where item.pathExtension.lowercased() == "zip" {
            let nestedDest = destination.appendingPathComponent(item.deletingPathExtension().lastPathComponent)
            try fileManager.createDirectory(at: nestedDest, withIntermediateDirectories: true)
            try extractZip(item, to: nestedDest, req: req, depth: depth + 1)
            try fileManager.removeItem(at: item)
        }
    }

    private func startProcessing(outputURL: URL, req: Request, state: ProcessingState) async throws -> ProcessingState {
        var state = state
        guard let folderURL = state.selectedFolderURL else {
            throw Abort(.badRequest, reason: "No folder selected")
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

        let session = try PhotogrammetrySession(input: imagesFolder, configuration: configuration)
        state.photogrammetrySession = session
        await req.storage.setWithAsyncShutdown(ProcessingStateKey.self, to: state)

        let timeoutTask = Task {
            try await Task.sleep(nanoseconds: 600_000_000_000)
            throw Abort(.internalServerError, reason: "Processing timed out after 600 seconds")
        }

        let processingTask = Task.detached { [weak self] in
            guard let self = self else { return }
            do {
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
                timeoutTask.cancel()
                throw error
            }
        }

        do {
            try session.process(requests: [.modelFile(url: outputURL, detail: optimalDetailLevel)])
            try await processingTask.value
            timeoutTask.cancel()
            return state
        } catch {
            timeoutTask.cancel()
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
    ) async -> ProcessingState {
        var newState = currentState
        
        switch output {
        case .processingComplete:
            newState.statusMessage = "Processing complete"
            newState.isProcessing = false
            newState.progress = 1.0
            req.logger.info("Processing completed successfully")
            
        case .requestComplete(_, let result):
            if case .modelFile(url: let url) = result {
                try? FileManager.default.moveItem(at: url, to: outputURL)
                newState.statusMessage = "Model saved"
                newState.isProcessing = false
            }
            
        case .requestProgress(_, let fractionComplete):
            newState.progress = fractionComplete
            let previousStage = newState.processingStage
            updateProcessingStage(fractionComplete, state: &newState, req: req)
            
            if previousStage != newState.processingStage || fractionComplete.truncatingRemainder(dividingBy: 0.05) < 0.01 {
                req.logger.info("Progress: \(String(format: "%.1f", fractionComplete * 100))% - \(newState.processingStage)")
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
        
        return newState
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
            throw error
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
            throw error
        }
    }

    private func prepareInputFolder(_ folderURL: URL, usePLY: Bool, req: Request) throws -> (tempFolder: URL, imagesFolder: URL) {
        let tempFolder = FileManager.default.temporaryDirectory
            .appendingPathComponent("photogrammetry_input_\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempFolder, withIntermediateDirectories: true)

        guard let imagesFolder = getImagesFolder(from: folderURL, req: req) else {
            throw Abort(.badRequest, reason: "Missing images folder. Ensure the ZIP contains an 'images' folder with at least 10 JPEG images and corresponding pose files.")
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
            throw Abort(.badRequest, reason: """
                Need at least 10 valid images (found \(processedCount)). 
                \(skippedDueToQuality > 0 ? "\(skippedDueToQuality) were skipped due to low quality." : "")
                """)
        }
        
        return tempFolder
    }

    private func validatePoseJSON(_ data: Data, req: Request) throws -> [[Double]] {
        guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let matrix = json["transform"] as? [[Double]] else {
            throw Abort(.badRequest, reason: "Missing or invalid transform matrix")
        }
        
        guard matrix.count == 4, matrix.allSatisfy({ $0.count == 4 }) else {
            throw Abort(.badRequest, reason: "Matrix must be 4x4")
        }
        
        for row in matrix {
            for value in row {
                if value.isNaN || value.isInfinite {
                    throw Abort(.badRequest, reason: "Matrix contains invalid numbers")
                }
            }
        }
        
        let simdMatrix = convertToSIMDMatrix(matrix, req: req)
        let determinant = abs(simdMatrix.determinant)
        guard determinant > 1e-6 else {
            throw Abort(.badRequest, reason: "Degenerate transform matrix")
        }
        
        return matrix
    }

    private func embedPoseData(in sourceURL: URL, to destinationURL: URL, with transform: simd_float4x4, poseJson: [String: Any]?, req: Request) throws {
        guard let source = CGImageSourceCreateWithURL(sourceURL as CFURL, nil) else {
            throw Abort(.internalServerError, reason: "Failed to create image source")
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
        }

        metadata[kCGImagePropertyExifDictionary as String] = exifDict

        if let source = CGImageSourceCreateWithData(imageData as CFData, nil) {
            CGImageDestinationAddImageFromSource(destination, source, 0, metadata as CFDictionary)
            guard CGImageDestinationFinalize(destination) else {
                throw Abort(.internalServerError, reason: "Failed to save image with pose data")
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
                    return ext == "jpg" || ext == "jpeg"
                }
                
                let hasPoseFiles = imageFiles.contains { imageURL in
                    let baseName = imageURL.deletingPathExtension().lastPathComponent
                    let poseURL = currentURL.appendingPathComponent("\(baseName)_pose.json")
                    return fileManager.fileExists(atPath: poseURL.path)
                }
                
                if imageFiles.count >= 10 {
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
            throw error
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
