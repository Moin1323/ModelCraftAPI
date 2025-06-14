import Vapor

func routes(_ app: Application) async throws {
    // Basic test routes
    app.get { req async in
        "It works!"
    }

    app.get("hello") { req async -> String in
        "Hello, world!"
    }

    // API routes group
    let api = app.grouped("api")
    
    // Process model endpoint
    api.post("process") { req -> EventLoopFuture<Response> in
        let promise = req.eventLoop.makePromise(of: Response.self)
        
        Task {
            let controller = await ModelProcessingController()
            do {
                let response = try await controller.processModel(req)
                promise.succeed(response)
            } catch {
                promise.fail(error)
            }
        }
        
        return promise.futureResult
    }
    
    // Progress endpoint
    api.get("progress") { req -> EventLoopFuture<ProgressResponse> in
        let promise = req.eventLoop.makePromise(of: ProgressResponse.self)
        
        Task {
            let controller = await ModelProcessingController()
            do {
                let progressResponse = try await controller.getProgress(req)
                promise.succeed(progressResponse)
            } catch {
                let defaultResponse = ProgressResponse(
                    statusMessage: "Error: \(error.localizedDescription)",
                    progress: 0.0,
                    processingStage: "Error",
                    isProcessing: false
                )
                promise.succeed(defaultResponse)
            }
        }
        
        return promise.futureResult
    }
}
