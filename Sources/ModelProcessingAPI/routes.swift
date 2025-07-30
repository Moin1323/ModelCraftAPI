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
    api.post("process") { req async throws -> Response in
        req.logger.info("Received /api/process request")
        let controller = await ModelProcessingController()
        return try await controller.processModel(req)
    }
    
    // Progress endpoint
    api.get("progress") { req async throws -> ProgressResponse in
        let controller = await ModelProcessingController()
        print(controller)
        
        let res =  try await controller.getProgress(req)
        print(res)
        return res
    }

    // <-- Add this test route:
    api.get("ping") { req async -> String in
        print("Ping endpoint hit!")
        return "pong"
    }
}
