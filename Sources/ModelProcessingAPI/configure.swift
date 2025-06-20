import Vapor

public func configure(_ app: Application) async throws {
    // 🔥 Bind to 0.0.0.0 instead of localhost
    app.http.server.configuration.hostname = "0.0.0.0"

    // Optional: change the port too (default is 8080)
    // app.http.server.configuration.port = 8080

    // Set global body size limit to 600MB
    app.routes.defaultMaxBodySize = "600mb"

    // Serve files from Public/ directory
    app.middleware.use(FileMiddleware(publicDirectory: app.directory.publicDirectory))

    // Register routes
    try await routes(app)
}
