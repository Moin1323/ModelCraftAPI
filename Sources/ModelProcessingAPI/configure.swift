import Vapor

public func configure(_ app: Application) async throws {
    // Set global body size limit to 100MB
    app.routes.defaultMaxBodySize = "600mb"

    // Serve files from Public/ directory (optional)
    app.middleware.use(FileMiddleware(publicDirectory: app.directory.publicDirectory))

    // Register routes
    try await routes(app)
}
