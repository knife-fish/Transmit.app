import SwiftUI

@main
struct TransmitApp: App {
    init() {
        AppConfiguration.validate()
    }

    var body: some Scene {
        WindowGroup {
            ContentView()
        }
        .defaultSize(width: 1440, height: 900)
        .windowResizability(.contentSize)
    }
}
