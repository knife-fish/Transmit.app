import Testing
import Foundation
@testable import Transmit

private final class TestRemoteClient: RemoteClient, @unchecked Sendable {
    private let localFileBrowser = LocalFileBrowserService()
    private let remoteRootURL: URL
    private let displayHost: String
    private let uploadChunkDelay: TimeInterval
    private let downloadChunkDelay: TimeInterval
    private let lock = NSLock()
    private var failingUploadNames: Set<String>

    init(
        remoteRootURL: URL,
        displayHost: String = "test-sftp.local",
        uploadChunkDelay: TimeInterval = 0,
        downloadChunkDelay: TimeInterval = 0,
        failingUploadNames: Set<String> = []
    ) {
        self.remoteRootURL = remoteRootURL.standardizedFileURL
        self.displayHost = displayHost
        self.uploadChunkDelay = uploadChunkDelay
        self.downloadChunkDelay = downloadChunkDelay
        self.failingUploadNames = failingUploadNames
    }

    func setFailingUploadNames(_ names: Set<String>) {
        lock.lock()
        failingUploadNames = names
        lock.unlock()
    }

    func makeInitialLocation(relativeTo localDirectoryURL: URL) -> RemoteLocation {
        makeLocation(for: remoteRootURL)
    }

    func makeLocation(for directoryURL: URL) -> RemoteLocation {
        let standardizedURL = directoryURL.standardizedFileURL
        return RemoteLocation(
            id: standardizedURL.path(percentEncoded: false),
            path: "sftp://\(displayHost)\(standardizedURL.path(percentEncoded: false))",
            remotePath: standardizedURL.path(percentEncoded: false),
            directoryURL: standardizedURL
        )
    }

    func parentLocation(of location: RemoteLocation) -> RemoteLocation? {
        guard let directoryURL = location.directoryURL else { return nil }
        let standardizedURL = directoryURL.standardizedFileURL
        guard standardizedURL.path != remoteRootURL.path else { return nil }
        return makeLocation(for: standardizedURL.deletingLastPathComponent())
    }

    func location(for item: BrowserItem, from currentLocation: RemoteLocation) -> RemoteLocation? {
        guard item.isDirectory else { return nil }
        return makeLocation(for: URL(fileURLWithPath: item.pathDescription))
    }

    func loadDirectorySnapshot(in location: RemoteLocation) throws -> RemoteDirectorySnapshot {
        guard let directoryURL = location.directoryURL else {
            throw CocoaError(.fileNoSuchFile)
        }
        let normalized = makeLocation(for: directoryURL)
        return RemoteDirectorySnapshot(
            location: normalized,
            items: try localFileBrowser.loadItems(in: directoryURL),
            homePath: makeLocation(for: remoteRootURL).remotePath
        )
    }

    func destinationDirectoryURL(for location: RemoteLocation) -> URL? {
        location.directoryURL
    }

    func uploadItem(
        at localURL: URL,
        to remoteLocation: RemoteLocation,
        conflictPolicy: TransferConflictPolicy,
        progress: (@Sendable (TransferProgressSnapshot) -> Void)?,
        isCancelled: (@Sendable () -> Bool)?
    ) throws -> RemoteUploadResult {
        lock.lock()
        let shouldFail = failingUploadNames.contains(localURL.lastPathComponent)
        lock.unlock()
        if shouldFail {
            throw NSError(domain: "TestRemoteClient", code: 7, userInfo: [
                NSLocalizedDescriptionKey: "Injected upload failure for \(localURL.lastPathComponent)."
            ])
        }

        guard let destinationDirectoryURL = remoteLocation.directoryURL else {
            throw CocoaError(.fileNoSuchFile)
        }
        let destinationURL = try LocalFileTransferService().destinationURL(
            forProposedName: localURL.lastPathComponent,
            in: destinationDirectoryURL,
            conflictPolicy: conflictPolicy
        )
        if conflictPolicy == .overwrite, FileManager.default.fileExists(atPath: destinationURL.path(percentEncoded: false)) {
            try LocalFileTransferService().deleteItem(at: destinationURL)
        }
        try copyFileWithProgress(
            from: localURL,
            to: destinationURL,
            progress: progress,
            isCancelled: isCancelled,
            chunkDelay: uploadChunkDelay
        )
        return RemoteUploadResult(
            remoteItemID: destinationURL.path(percentEncoded: false),
            destinationName: destinationURL.lastPathComponent,
            renamedForConflict: destinationURL.lastPathComponent != localURL.lastPathComponent
        )
    }

    func downloadItem(
        named name: String,
        at remotePath: String,
        toDirectory localDirectoryURL: URL,
        localFileTransfer: LocalFileTransferService,
        conflictPolicy: TransferConflictPolicy,
        progress: (@Sendable (TransferProgressSnapshot) -> Void)?,
        isCancelled: (@Sendable () -> Bool)?
    ) throws -> LocalFileTransferResult {
        let sourceURL = URL(fileURLWithPath: remotePath)
        let destinationURL = try localFileTransfer.destinationURL(
            forProposedName: name,
            in: localDirectoryURL,
            conflictPolicy: conflictPolicy
        )
        if conflictPolicy == .overwrite, FileManager.default.fileExists(atPath: destinationURL.path(percentEncoded: false)) {
            try localFileTransfer.deleteItem(at: destinationURL)
        }
        try copyFileWithProgress(
            from: sourceURL,
            to: destinationURL,
            progress: progress,
            isCancelled: isCancelled,
            chunkDelay: downloadChunkDelay
        )
        return LocalFileTransferResult(
            sourceURL: sourceURL,
            destinationURL: destinationURL,
            renamedForConflict: destinationURL.lastPathComponent != name
        )
    }

    func createDirectory(named proposedName: String, in remoteLocation: RemoteLocation) throws -> RemoteMutationResult {
        guard let directoryURL = remoteLocation.directoryURL else {
            throw CocoaError(.fileNoSuchFile)
        }
        let createdURL = try LocalFileTransferService().createDirectory(named: proposedName, in: directoryURL)
        return RemoteMutationResult(
            remoteItemID: createdURL.path(percentEncoded: false),
            destinationName: createdURL.lastPathComponent
        )
    }

    func renameItem(named originalName: String, at remotePath: String, to proposedName: String) throws -> RemoteMutationResult {
        let sourceURL = URL(fileURLWithPath: remotePath)
        let renamedURL = try LocalFileTransferService().renameItem(at: sourceURL, toName: proposedName)
        return RemoteMutationResult(
            remoteItemID: renamedURL.path(percentEncoded: false),
            destinationName: renamedURL.lastPathComponent
        )
    }

    func deleteItem(named name: String, at remotePath: String, isDirectory: Bool) throws {
        try LocalFileTransferService().deleteItem(at: URL(fileURLWithPath: remotePath))
    }

    private func copyFileWithProgress(
        from sourceURL: URL,
        to destinationURL: URL,
        progress: (@Sendable (TransferProgressSnapshot) -> Void)?,
        isCancelled: (@Sendable () -> Bool)?,
        chunkDelay: TimeInterval
    ) throws {
        let totalByteCount = try sourceURL.resourceValues(forKeys: [.fileSizeKey]).fileSize.map(Int64.init)
        FileManager.default.createFile(atPath: destinationURL.path(percentEncoded: false), contents: nil)

        let inputHandle = try FileHandle(forReadingFrom: sourceURL)
        let outputHandle = try FileHandle(forWritingTo: destinationURL)
        defer {
            try? inputHandle.close()
            try? outputHandle.close()
        }

        let chunkSize = 32 * 1024
        var completedByteCount: Int64 = 0
        progress?(.init(completedByteCount: 0, totalByteCount: totalByteCount))

        while true {
            if isCancelled?() == true {
                try? FileManager.default.removeItem(at: destinationURL)
                throw CancellationError()
            }
            let data = try inputHandle.read(upToCount: chunkSize) ?? Data()
            if data.isEmpty { break }
            try outputHandle.write(contentsOf: data)
            completedByteCount += Int64(data.count)
            progress?(.init(completedByteCount: completedByteCount, totalByteCount: totalByteCount))
            if chunkDelay > 0 {
                Thread.sleep(forTimeInterval: chunkDelay)
            }
        }
    }
}

struct TransmitTests {
    private func eventually(
        timeout: TimeInterval = 2.0,
        pollInterval: UInt64 = 50_000_000,
        _ condition: @escaping @MainActor () -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if await MainActor.run(body: condition) {
                return
            }
            try await Task.sleep(nanoseconds: pollInterval)
        }

        Issue.record("Timed out waiting for asynchronous state to settle.")
    }

    @Test func workspaceBootstrapsSampleData() async throws {
        let state = await MainActor.run { TransmitWorkspaceState() }

        await MainActor.run {
            #expect(state.selectedServer != nil)
            #expect(!state.servers.isEmpty)
            #expect(!state.localItems.isEmpty)
            #expect(!state.remoteItems.isEmpty)
            #expect(state.recentTransfers.isEmpty)
            #expect(!state.localPathDisplayName.isEmpty)
            #expect(!state.remotePathDisplayName.isEmpty)
            #expect(state.remotePathDisplayName.hasPrefix("sftp://"))
            #expect(state.selectedLocalItemID != nil)
            #expect(state.selectedRemoteItemID != nil)
        }
    }

    @Test func workspaceDefaultsToComfortableDensityAndCanSwitchModes() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let workspacePreferencesURL = baseURL.appendingPathComponent("WorkspacePreferences.json")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                workspacePreferenceStore: JSONWorkspacePreferenceStore(fileURL: workspacePreferencesURL),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            #expect(state.browserDensity == .comfortable)
            state.setBrowserDensity(.compact)
            #expect(state.browserDensity == .compact)
            state.setBrowserDensity(.ultraCompact)
            #expect(state.browserDensity == .ultraCompact)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func connectionStateOnlyAppliesToSessionServer() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let remoteChildURL = remoteURL.appendingPathComponent("Inbox", isDirectory: true)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteChildURL, withIntermediateDirectories: true)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                remoteClientFactory: { server, draft, localFileBrowser in
                    RemoteSessionServices(
                        client: MockRemoteClient(
                            localFileBrowser: localFileBrowser,
                            displayHost: draft.host.isEmpty ? server.endpoint : draft.host
                        )
                    )
                },
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }
        let sessionServer = await MainActor.run { state.servers[0] }
        let otherServer = await MainActor.run { state.servers[1] }

        await MainActor.run {
            state.selectServer(sessionServer)
            state.connectRemoteSession()
        }

        try await eventually {
            state.connectionState(for: sessionServer) == .connected("\(sessionServer.username)@\(sessionServer.endpoint)")
        }

        await MainActor.run {
            #expect(state.connectionState(for: sessionServer) == .connected("\(sessionServer.username)@\(sessionServer.endpoint)"))
            #expect(state.connectionState(for: otherServer) == .idle)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func localBrowserCanNavigateIntoFolder() async throws {
        let rootURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let childURL = rootURL.appendingPathComponent("Child", isDirectory: true)
        try FileManager.default.createDirectory(at: childURL, withIntermediateDirectories: true)
        try "fixture".write(to: rootURL.appendingPathComponent("notes.txt"), atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                initialLocalDirectoryURL: rootURL
            )
        }

        try await MainActor.run {
            #expect(state.localItems.contains(where: { $0.id == childURL.path(percentEncoded: false) }))
            let originalPath = state.localPathDisplayName
            state.selectLocalItem(id: childURL.path(percentEncoded: false))
            state.openLocalSelection()

            #expect(state.localPathDisplayName != originalPath)
            #expect(state.localDirectoryURL.standardizedFileURL == childURL.standardizedFileURL)
        }

        try? FileManager.default.removeItem(at: rootURL)
    }

    @Test func remoteBrowserCanNavigateIntoFolder() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let childURL = remoteURL.appendingPathComponent("Packages", isDirectory: true)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: childURL, withIntermediateDirectories: true)
        try "artifact".write(to: remoteURL.appendingPathComponent("build.zip"), atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            #expect(state.remoteItems.contains(where: { $0.id == childURL.path(percentEncoded: false) }))
            let originalPath = state.remotePathDisplayName
            state.selectRemoteItem(id: childURL.path(percentEncoded: false))
            state.openRemoteSelection()

            #expect(state.remotePathDisplayName != originalPath)
            #expect(state.remoteLocation.directoryURL?.standardizedFileURL == childURL.standardizedFileURL)
            #expect(state.remotePathDisplayName.hasPrefix("sftp://"))
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func copyFocusedSelectionCopiesIntoOtherPaneAndLogsActivity() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = localURL.appendingPathComponent("release-notes.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "ship it".write(to: sourceFileURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.selectLocalItem(id: sourceFileURL.path(percentEncoded: false))
            state.copyFocusedSelectionToOtherPane()
        }

        try await eventually {
            let copiedItemID = remoteURL
                .appendingPathComponent("release-notes.txt")
                .path(percentEncoded: false)
            return state.remoteItems.contains(where: { $0.id == copiedItemID })
        }

        try await MainActor.run {
            let copiedItemID = remoteURL
                .appendingPathComponent("release-notes.txt")
                .path(percentEncoded: false)

            #expect(state.remoteItems.contains(where: { $0.id == copiedItemID }))
            #expect(state.selectedRemoteItemID == copiedItemID)
            #expect(state.recentTransfers.first?.status == .completed)
            #expect(state.recentTransfers.first?.title == "release-notes.txt")
            #expect(state.transferFeedback?.message.contains(sourceFileURL.lastPathComponent) == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func copyFocusedSelectionRenamesWhenDestinationAlreadyExists() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = localURL.appendingPathComponent("report.txt")
        let existingDestinationURL = remoteURL.appendingPathComponent("report.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "new report".write(to: sourceFileURL, atomically: true, encoding: .utf8)
        try "old report".write(to: existingDestinationURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.remoteSessionStatus = .connected("deploy@app.example.com")
            state.selectLocalItem(id: sourceFileURL.path(percentEncoded: false))
            state.copyFocusedSelectionToOtherPane()
        }

        await MainActor.run {
            #expect(state.transferConflictResolutionRequest?.conflictingNames == ["report.txt"])
            state.resolveTransferConflict(with: .rename)
        }

        try await eventually {
            let renamedItemID = remoteURL
                .appendingPathComponent("report 2.txt")
                .path(percentEncoded: false)
            return state.remoteItems.contains(where: { $0.id == renamedItemID })
        }

        try await MainActor.run {
            let renamedItemID = remoteURL
                .appendingPathComponent("report 2.txt")
                .path(percentEncoded: false)

            #expect(state.remoteItems.contains(where: { $0.id == renamedItemID }))
            #expect(state.selectedRemoteItemID == renamedItemID)
            #expect(state.transferFeedback?.message.contains("report 2.txt") == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func handleDropCopiesIntoTargetPane() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = localURL.appendingPathComponent("dragged.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "drag me".write(to: sourceFileURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.remoteSessionStatus = .connected("deploy@app.example.com")
        }

        let accepted = await MainActor.run {
            state.handleDrop(of: [sourceFileURL], into: .remote)
        }

        try await eventually {
            state.remoteItems.contains(where: { $0.name == "dragged.txt" }) &&
            state.selectedRemoteItemID != nil
        }

        try await MainActor.run {
            let copiedItem = state.remoteItems.first(where: { $0.name == "dragged.txt" })

            #expect(accepted)
            #expect(copiedItem != nil)
            #expect(state.selectedRemoteItemID == copiedItem?.id)
            #expect(state.focusedPane == .remote)
            #expect(state.transferFeedback?.message.contains(sourceFileURL.lastPathComponent) == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func handleDropUploadsFolderIntoRemotePane() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFolderURL = localURL.appendingPathComponent("Assets", isDirectory: true)
        let nestedFileURL = sourceFolderURL.appendingPathComponent("hero.txt")

        try FileManager.default.createDirectory(at: sourceFolderURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "hero".write(to: nestedFileURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.remoteSessionStatus = .connected("deploy@app.example.com")
        }

        let accepted = await MainActor.run {
            state.handleDrop(of: [sourceFolderURL], into: .remote)
        }

        try await eventually {
            let copiedDirectoryURL = remoteURL.appendingPathComponent("Assets", isDirectory: true)
            let copiedFileURL = copiedDirectoryURL.appendingPathComponent("hero.txt")
            let copiedDirectoryID = copiedDirectoryURL.path(percentEncoded: false)
            return FileManager.default.fileExists(atPath: copiedDirectoryURL.path) &&
                FileManager.default.fileExists(atPath: copiedFileURL.path) &&
                state.remoteItems.contains(where: { $0.id == copiedDirectoryID && $0.isDirectory })
        }

        try await MainActor.run {
            let copiedDirectoryID = remoteURL.appendingPathComponent("Assets", isDirectory: true).path(percentEncoded: false)
            #expect(accepted)
            #expect(state.remoteItems.contains(where: { $0.id == copiedDirectoryID && $0.isDirectory }))
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func handleDropUploadsMultipleFilesIntoRemotePane() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let stateURL = baseURL.appendingPathComponent("State", isDirectory: true)
        let firstURL = localURL.appendingPathComponent("one.txt")
        let secondURL = localURL.appendingPathComponent("two.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: stateURL, withIntermediateDirectories: true)
        try "one".write(to: firstURL, atomically: true, encoding: .utf8)
        try "two".write(to: secondURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: JSONSavedServerStore(
                    fileURL: stateURL.appendingPathComponent("SavedServers.json", isDirectory: false)
                ),
                favoritePlaceStore: JSONFavoritePlaceStore(
                    fileURL: stateURL.appendingPathComponent("FavoritePlaces.json", isDirectory: false)
                ),
                workspacePreferenceStore: JSONWorkspacePreferenceStore(
                    fileURL: stateURL.appendingPathComponent("WorkspacePreferences.json", isDirectory: false)
                ),
                siteUsageStore: JSONSiteUsageStore(
                    fileURL: stateURL.appendingPathComponent("SiteUsage.json", isDirectory: false)
                ),
                credentialStore: InMemoryServerCredentialStore(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.remoteSessionStatus = .connected("deploy@app.example.com")
        }

        let accepted = await MainActor.run {
            state.handleDrop(of: [firstURL, secondURL], into: .remote)
        }

        try await eventually {
            FileManager.default.fileExists(atPath: remoteURL.appendingPathComponent("one.txt").path) &&
            FileManager.default.fileExists(atPath: remoteURL.appendingPathComponent("two.txt").path) &&
            state.remoteItems.contains(where: { $0.name == "one.txt" }) &&
            state.remoteItems.contains(where: { $0.name == "two.txt" })
        }

        try await MainActor.run {
            #expect(accepted)
            #expect(state.remoteItems.contains(where: { $0.name == "one.txt" }))
            #expect(state.remoteItems.contains(where: { $0.name == "two.txt" }))
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func handleRemoteDropDownloadsIntoLocalPane() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = remoteURL.appendingPathComponent("server.log")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "remote log".write(to: sourceFileURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        let accepted = await MainActor.run {
            state.handleRemoteDrop(
                of: [
                    RemoteDragItem(
                        id: sourceFileURL.path(percentEncoded: false),
                        name: "server.log",
                        pathDescription: sourceFileURL.path(percentEncoded: false),
                        isDirectory: false
                    )
                ],
                into: .local
            )
        }

        try await eventually {
            let downloadedItemID = localURL.appendingPathComponent("server.log").path(percentEncoded: false)
            return state.localItems.contains(where: { $0.id == downloadedItemID })
        }

        try await MainActor.run {
            let downloadedItemID = localURL.appendingPathComponent("server.log").path(percentEncoded: false)

            #expect(accepted)
            #expect(state.localItems.contains(where: { $0.id == downloadedItemID }))
            #expect(state.selectedLocalItemID == downloadedItemID)
            #expect(state.focusedPane == .local)
            #expect(state.transferFeedback?.message.contains("server.log") == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func handleRemoteDropDownloadsFolderIntoLocalPane() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFolderURL = remoteURL.appendingPathComponent("Logs", isDirectory: true)
        let nestedFileURL = sourceFolderURL.appendingPathComponent("app.log")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: sourceFolderURL, withIntermediateDirectories: true)
        try "remote log".write(to: nestedFileURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        let accepted = await MainActor.run {
            state.handleRemoteDrop(
                of: [
                    RemoteDragItem(
                        id: sourceFolderURL.path(percentEncoded: false),
                        name: "Logs",
                        pathDescription: sourceFolderURL.path(percentEncoded: false),
                        isDirectory: true
                    )
                ],
                into: .local
            )
        }

        try await eventually {
            let downloadedDirectoryURL = localURL.appendingPathComponent("Logs", isDirectory: true)
            let downloadedFileURL = downloadedDirectoryURL.appendingPathComponent("app.log")
            return FileManager.default.fileExists(atPath: downloadedDirectoryURL.path) &&
                FileManager.default.fileExists(atPath: downloadedFileURL.path)
        }

        try await MainActor.run {
            let downloadedDirectoryID = localURL.appendingPathComponent("Logs", isDirectory: true).path(percentEncoded: false)
            #expect(accepted)
            #expect(state.localItems.contains(where: { $0.id == downloadedDirectoryID && $0.isDirectory }))
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func handleRemoteDropDownloadsMultipleFilesIntoLocalPane() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let firstURL = remoteURL.appendingPathComponent("alpha.txt")
        let secondURL = remoteURL.appendingPathComponent("beta.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "alpha".write(to: firstURL, atomically: true, encoding: .utf8)
        try "beta".write(to: secondURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        let accepted = await MainActor.run {
            state.handleRemoteDrop(
                of: [
                    RemoteDragItem(
                        id: firstURL.path(percentEncoded: false),
                        name: "alpha.txt",
                        pathDescription: firstURL.path(percentEncoded: false),
                        isDirectory: false
                    ),
                    RemoteDragItem(
                        id: secondURL.path(percentEncoded: false),
                        name: "beta.txt",
                        pathDescription: secondURL.path(percentEncoded: false),
                        isDirectory: false
                    )
                ],
                into: .local
            )
        }

        try await eventually {
            FileManager.default.fileExists(atPath: localURL.appendingPathComponent("alpha.txt").path) &&
            FileManager.default.fileExists(atPath: localURL.appendingPathComponent("beta.txt").path)
        }

        try await MainActor.run {
            #expect(accepted)
            #expect(state.localItems.contains(where: { $0.name == "alpha.txt" }))
            #expect(state.localItems.contains(where: { $0.name == "beta.txt" }))
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func renameRequestRenamesSelectedItem() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = localURL.appendingPathComponent("draft.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "rename me".write(to: sourceFileURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            state.selectLocalItem(id: sourceFileURL.path(percentEncoded: false))
            state.beginRenamingFocusedSelection()
            state.renameRequest?.proposedName = "published.txt"
            state.submitRenameRequest()

            let renamedItemID = localURL.appendingPathComponent("published.txt").path(percentEncoded: false)

            #expect(state.localItems.contains(where: { $0.id == renamedItemID }))
            #expect(state.selectedLocalItemID == renamedItemID)
            #expect(state.transferFeedback?.message.contains("draft.txt") == true)
            #expect(state.transferFeedback?.message.contains("published.txt") == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func remoteRenameRequestRenamesSelectedItem() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = remoteURL.appendingPathComponent("draft.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "rename me remotely".write(to: sourceFileURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.selectRemoteItem(id: sourceFileURL.path(percentEncoded: false))
            state.beginRenamingFocusedSelection()
            state.renameRequest?.proposedName = "published.txt"
            state.submitRenameRequest()
        }

        try await eventually {
            let renamedItemID = remoteURL.appendingPathComponent("published.txt").path(percentEncoded: false)
            return state.remoteItems.contains(where: { $0.id == renamedItemID })
        }

        try await MainActor.run {
            let renamedItemID = remoteURL.appendingPathComponent("published.txt").path(percentEncoded: false)

            #expect(state.remoteItems.contains(where: { $0.id == renamedItemID }))
            #expect(state.selectedRemoteItemID == renamedItemID)
            #expect(state.transferFeedback?.message.contains("draft.txt") == true)
            #expect(state.transferFeedback?.message.contains("published.txt") == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func deleteRequestRemovesSelectedItem() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = localURL.appendingPathComponent("obsolete.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "delete me".write(to: sourceFileURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            state.selectLocalItem(id: sourceFileURL.path(percentEncoded: false))
            state.requestDeleteFocusedSelection()
            state.confirmDeleteRequest()

            #expect(!state.localItems.contains(where: { $0.id == sourceFileURL.path(percentEncoded: false) }))
            #expect(state.transferFeedback?.message.contains("obsolete.txt") == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func remoteDeleteRequestRemovesSelectedItem() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = remoteURL.appendingPathComponent("obsolete.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "delete me remotely".write(to: sourceFileURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.selectRemoteItem(id: sourceFileURL.path(percentEncoded: false))
            state.requestDeleteFocusedSelection()
            state.confirmDeleteRequest()
        }

        try await eventually {
            !state.remoteItems.contains(where: { $0.id == sourceFileURL.path(percentEncoded: false) }) &&
            state.selectedRemoteItemID == nil || !state.remoteItems.contains(where: { $0.id == state.selectedRemoteItemID })
        }

        try await MainActor.run {
            #expect(!state.remoteItems.contains(where: { $0.id == sourceFileURL.path(percentEncoded: false) }))
            #expect(state.transferFeedback?.message.contains("obsolete.txt") == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func createFolderRequestCreatesLocalFolder() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            state.focusedPane = .local
            state.beginCreatingFolderInFocusedPane()
            state.createFolderRequest?.proposedName = "Archives"
            state.submitCreateFolderRequest()

            let folderID = localURL.appendingPathComponent("Archives", isDirectory: true).path(percentEncoded: false)

            #expect(state.localItems.contains(where: { $0.id == folderID }))
            #expect(state.selectedLocalItemID == folderID)
            #expect(state.transferFeedback?.message.contains("Archives") == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func createFolderRequestCreatesRemoteFolder() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.remoteSessionStatus = .connected("deploy@app.example.com")
        }

        await MainActor.run {
            state.focusedPane = .remote
            state.beginCreatingFolderInFocusedPane()
            state.createFolderRequest?.proposedName = "Releases"
            state.submitCreateFolderRequest()
        }

        try await eventually {
            state.remoteItems.contains(where: { $0.name == "Releases" && $0.isDirectory }) &&
            state.selectedRemoteItemID != nil
        }

        try await MainActor.run {
            let folder = state.remoteItems.first(where: { $0.name == "Releases" && $0.isDirectory })

            #expect(folder != nil)
            #expect(state.selectedRemoteItemID == folder?.id)
            #expect(state.transferFeedback?.message.contains("Releases") == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func selectingServerUpdatesConnectionDraft() async throws {
        let state = await MainActor.run { TransmitWorkspaceState() }

        try await MainActor.run {
            let server = TransmitWorkspaceState.sampleServers[0]
            state.selectServer(server)

            #expect(state.selectedServer == server)
            #expect(state.connectionDraft.host == server.endpoint)
            #expect(state.connectionDraft.port == String(server.port))
            #expect(state.connectionDraft.username == server.username)
            #expect(state.connectionDraft.authenticationMode == server.authenticationMode)
        }
    }

    @Test func selectingAnotherServerKeepsExistingRemoteSession() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let remoteChildURL = remoteURL.appendingPathComponent("Inbox", isDirectory: true)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteChildURL, withIntermediateDirectories: true)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                remoteClientFactory: { server, draft, localFileBrowser in
                    RemoteSessionServices(
                        client: MockRemoteClient(
                            localFileBrowser: localFileBrowser,
                            displayHost: draft.host.isEmpty ? server.endpoint : draft.host
                        )
                    )
                },
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        let firstServer = await MainActor.run { state.servers[0] }
        let secondServer = await MainActor.run { state.servers[1] }

        await MainActor.run {
            state.selectServer(firstServer)
            state.connectRemoteSession()
        }

        try await eventually {
            state.remoteSessionStatus == .connected("\(firstServer.username)@\(firstServer.endpoint)")
        }

        try await MainActor.run {
            #expect(!state.remoteItems.isEmpty)
            let originalRemotePath = state.remotePathDisplayName

            state.selectServer(secondServer)

            #expect(state.selectedServer == secondServer)
            #expect(state.remoteSessionStatus == .connected("\(firstServer.username)@\(firstServer.endpoint)"))
            #expect(!state.remoteItems.isEmpty)
            #expect(state.remotePathDisplayName == originalRemotePath)
            #expect(state.connectionState(for: firstServer) == .connected("\(firstServer.username)@\(firstServer.endpoint)"))
            #expect(state.connectionState(for: secondServer) == .idle)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func connectRemoteSessionPromotesMockSFTPSession() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let remoteChildURL = remoteURL.appendingPathComponent("Inbox", isDirectory: true)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteChildURL, withIntermediateDirectories: true)

        let remoteClient = MockRemoteClient(
            localFileBrowser: LocalFileBrowserService(),
            displayHost: "bootstrap.local"
        )

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                remoteClient: remoteClient,
                remoteClientFactory: { server, draft, localFileBrowser in
                    RemoteSessionServices(
                        client: MockRemoteClient(
                            localFileBrowser: localFileBrowser,
                            displayHost: draft.host.isEmpty ? server.endpoint : draft.host
                        )
                    )
                },
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            let server = TransmitWorkspaceState.sampleServers[0]
            state.selectServer(server)
            state.connectionDraft.host = "sftp.example.com"
            state.connectionDraft.username = "release"
            state.connectRemoteSession()
        }

        try await eventually {
            state.remoteSessionStatus == .connected("release@sftp.example.com")
        }

        try await eventually {
            !state.remoteItems.isEmpty
        }

        try await MainActor.run {
            #expect(state.remotePathDisplayName.hasPrefix("sftp://sftp.example.com"))
            #expect(state.remoteSessionStatus == .connected("release@sftp.example.com"))
            #expect(!state.remoteItems.isEmpty)
            #expect(state.transferFeedback?.message.contains("Production SFTP") == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func saveConnectionDraftPersistsServersAndPassword() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let storeURL = baseURL.appendingPathComponent("SavedServers.json")
        let savedServerStore = JSONSavedServerStore(fileURL: storeURL)
        let credentialStore = InMemoryServerCredentialStore()

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)

        let firstState = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: savedServerStore,
                credentialStore: credentialStore,
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        let savedServer = try await MainActor.run {
            firstState.beginCreatingSite()
            firstState.connectionDraft.name = "Release Box"
            firstState.connectionDraft.host = "files.example.com"
            firstState.connectionDraft.port = "22"
            firstState.connectionDraft.username = "ship"
            firstState.connectionDraft.authenticationMode = .sshKey
            firstState.connectionDraft.privateKeyPath = "/Users/demo/.ssh/deploy.pem"
            firstState.connectionDraft.publicKeyPath = "/Users/demo/.ssh/deploy.pem.pub"
            firstState.connectionDraft.password = "top-secret"
            firstState.connectionDraft.addressPreference = .ipv6
            firstState.connectionDraft.defaultLocalDirectoryPath = localURL.path(percentEncoded: false)
            firstState.connectionDraft.defaultRemotePath = "/Inbox"
            return firstState.saveConnectionDraftAsSite()
        }

        #expect(savedServer != nil)

        let secondState = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: savedServerStore,
                credentialStore: credentialStore,
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        let persistedServer = await MainActor.run {
            secondState.servers.first(where: { $0.name == "Release Box" })
        }

        try await MainActor.run {
            #expect(persistedServer?.endpoint == "files.example.com")
            #expect(persistedServer?.authenticationMode == .sshKey)
            #expect(persistedServer?.privateKeyPath == "/Users/demo/.ssh/deploy.pem")
            #expect(persistedServer?.publicKeyPath == "/Users/demo/.ssh/deploy.pem.pub")
            #expect(persistedServer?.addressPreference == .ipv6)
            #expect(persistedServer?.defaultLocalDirectoryPath == localURL.path(percentEncoded: false))
            #expect(persistedServer?.defaultRemotePath == "/Inbox")
            #expect(secondState.selectedServer?.name == "Release Box")
            #expect(secondState.connectionDraft.host == "files.example.com")
            #expect(secondState.connectionDraft.username == "ship")
            #expect(secondState.connectionDraft.authenticationMode == .sshKey)
            #expect(secondState.connectionDraft.privateKeyPath == "/Users/demo/.ssh/deploy.pem")
            #expect(secondState.connectionDraft.publicKeyPath == "/Users/demo/.ssh/deploy.pem.pub")
            #expect(secondState.connectionDraft.password == "top-secret")
            #expect(secondState.connectionDraft.addressPreference == .ipv6)
            #expect(secondState.connectionDraft.defaultLocalDirectoryPath == localURL.path(percentEncoded: false))
            #expect(secondState.connectionDraft.defaultRemotePath == "/Inbox")
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func saveConnectionDraftRejectsSSHKeySiteWithoutPrivateKey() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let storeURL = baseURL.appendingPathComponent("SavedServers.json")
        let savedServerStore = JSONSavedServerStore(fileURL: storeURL)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: savedServerStore,
                credentialStore: InMemoryServerCredentialStore(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        let savedServer = await MainActor.run {
            state.beginCreatingSite()
            state.connectionDraft.name = "Broken Key Site"
            state.connectionDraft.host = "files.example.com"
            state.connectionDraft.username = "ship"
            state.connectionDraft.authenticationMode = .sshKey
            state.connectionDraft.privateKeyPath = ""
            return state.saveConnectionDraftAsSite()
        }

        try await MainActor.run {
            #expect(savedServer == nil)
            #expect(state.servers.contains(where: { $0.name == "Broken Key Site" }) == false)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func connectRemoteSessionFallsBackToStoredPassword() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let remoteChildURL = remoteURL.appendingPathComponent("Inbox", isDirectory: true)
        let storeURL = baseURL.appendingPathComponent("SavedServers.json")
        let savedServerStore = JSONSavedServerStore(fileURL: storeURL)
        let credentialStore = InMemoryServerCredentialStore()
        let savedServer = ServerProfile(
            id: UUID(),
            name: "Stored Site",
            endpoint: "stored.example.com",
            port: 22,
            username: "stored-user",
            connectionKind: .sftp,
            systemImage: "server.rack",
            accentName: "Orange"
        )

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteChildURL, withIntermediateDirectories: true)
        try savedServerStore.saveServers([savedServer])
        try credentialStore.setPassword("stored-password", for: savedServer.id)

        let capturedPassword = Locked<String?>(nil)
        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                remoteClientFactory: { server, draft, localFileBrowser in
                    capturedPassword.set(draft.password)
                    return RemoteSessionServices(
                        client: MockRemoteClient(
                            localFileBrowser: localFileBrowser,
                            displayHost: draft.host.isEmpty ? server.endpoint : draft.host
                        )
                    )
                },
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: savedServerStore,
                credentialStore: credentialStore,
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.selectServer(savedServer)
            state.connectionDraft.password = ""
            state.connectRemoteSession()
        }

        try await eventually {
            state.remoteSessionStatus == .connected("stored-user@stored.example.com")
        }

        #expect(capturedPassword.value == "stored-password")

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func connectRemoteSessionAppliesConfiguredLocalAndRemoteDirectories() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let preferredLocalURL = localURL.appendingPathComponent("Preferred", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let remoteChildURL = remoteURL.appendingPathComponent("Inbox", isDirectory: true)
        let storeURL = baseURL.appendingPathComponent("SavedServers.json")
        let savedServerStore = JSONSavedServerStore(fileURL: storeURL)

        try FileManager.default.createDirectory(at: preferredLocalURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteChildURL, withIntermediateDirectories: true)
        try "artifact".write(to: remoteChildURL.appendingPathComponent("artifact.txt"), atomically: true, encoding: .utf8)

        let savedServer = ServerProfile(
            id: UUID(),
            name: "Preferred Paths",
            endpoint: "preferred.example.com",
            port: 22,
            username: "example",
            connectionKind: .sftp,
            addressPreference: .ipv4,
            defaultLocalDirectoryPath: preferredLocalURL.path(percentEncoded: false),
            defaultRemotePath: remoteChildURL.path(percentEncoded: false),
            systemImage: "server.rack",
            accentName: "Orange"
        )
        try savedServerStore.saveServers([savedServer])

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                remoteClientFactory: { server, draft, localFileBrowser in
                    RemoteSessionServices(
                        client: MockRemoteClient(
                            localFileBrowser: localFileBrowser,
                            displayHost: draft.host.isEmpty ? server.endpoint : draft.host
                        )
                    )
                },
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: savedServerStore,
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.selectServer(savedServer)
            state.connectRemoteSession()
        }

        try await eventually {
            state.remoteSessionStatus == .connected("example@preferred.example.com")
        }

        try await MainActor.run {
            #expect(state.localDirectoryURL.standardizedFileURL == preferredLocalURL.standardizedFileURL)
            #expect(state.remoteLocation.remotePath == remoteChildURL.path(percentEncoded: false))
            #expect(state.remoteItems.contains(where: { $0.name == "artifact.txt" }))
            #expect(state.connectionDraft.addressPreference == .ipv4)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func savedSiteUsagePersistsAcrossWorkspaceReload() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let remoteChildURL = remoteURL.appendingPathComponent("Inbox", isDirectory: true)
        let storeURL = baseURL.appendingPathComponent("SavedServers.json")
        let usageURL = baseURL.appendingPathComponent("SiteUsage.json")
        let savedServerStore = JSONSavedServerStore(fileURL: storeURL)
        let siteUsageStore = JSONSiteUsageStore(fileURL: usageURL)
        let credentialStore = InMemoryServerCredentialStore()
        let savedServer = ServerProfile(
            id: UUID(),
            name: "Release Box",
            endpoint: "files.example.com",
            port: 22,
            username: "ship",
            connectionKind: .sftp,
            systemImage: "server.rack",
            accentName: "Orange"
        )

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteChildURL, withIntermediateDirectories: true)
        try savedServerStore.saveServers([savedServer])

        let firstState = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                remoteClientFactory: { server, draft, localFileBrowser in
                    RemoteSessionServices(
                        client: MockRemoteClient(
                            localFileBrowser: localFileBrowser,
                            displayHost: draft.host.isEmpty ? server.endpoint : draft.host
                        )
                    )
                },
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: savedServerStore,
                siteUsageStore: siteUsageStore,
                credentialStore: credentialStore,
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            firstState.selectServer(savedServer)
            firstState.connectRemoteSession()
        }

        try await eventually {
            firstState.remoteSessionStatus == .connected("ship@files.example.com")
        }

        try await MainActor.run {
            #expect(firstState.siteUsage(for: savedServer)?.lastConnectionSummary == "ship@files.example.com")
            #expect(firstState.siteUsage(for: savedServer)?.lastConnectedAt != nil)
        }

        let secondState = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: savedServerStore,
                siteUsageStore: siteUsageStore,
                credentialStore: credentialStore,
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            #expect(secondState.siteUsage(for: savedServer)?.lastConnectionSummary == "ship@files.example.com")
            #expect(secondState.siteUsage(for: savedServer)?.lastConnectedAt != nil)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func deleteServerRemovesSavedSiteAndCredential() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let storeURL = baseURL.appendingPathComponent("SavedServers.json")
        let savedServerStore = JSONSavedServerStore(fileURL: storeURL)
        let credentialStore = InMemoryServerCredentialStore()
        let savedServer = ServerProfile(
            id: UUID(),
            name: "Delete Me",
            endpoint: "delete.example.com",
            port: 22,
            username: "deleter",
            connectionKind: .sftp,
            systemImage: "server.rack",
            accentName: "Orange"
        )

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try savedServerStore.saveServers([savedServer])
        try credentialStore.setPassword("remove-this", for: savedServer.id)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                remoteClientFactory: { server, draft, localFileBrowser in
                    RemoteSessionServices(
                        client: MockRemoteClient(
                            localFileBrowser: localFileBrowser,
                            displayHost: draft.host.isEmpty ? server.endpoint : draft.host
                        )
                    )
                },
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: savedServerStore,
                credentialStore: credentialStore,
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.selectServer(savedServer)
            state.connectRemoteSession()
        }

        try await eventually {
            state.remoteSessionStatus == .connected("deleter@delete.example.com")
        }

        try await MainActor.run {
            state.deleteServer(savedServer)

            #expect(!state.servers.contains(where: { $0.id == savedServer.id }))
            #expect(state.remoteSessionStatus == .idle)
            #expect(state.transferFeedback?.message.contains("Delete Me") == true)
        }

        let reloadedServers = try savedServerStore.loadServers()
        #expect(!reloadedServers.contains(where: { $0.id == savedServer.id }))
        #expect(try credentialStore.password(for: savedServer.id) == nil)

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func deleteServerRemovesPersistedSiteUsage() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let storeURL = baseURL.appendingPathComponent("SavedServers.json")
        let usageURL = baseURL.appendingPathComponent("SiteUsage.json")
        let savedServerStore = JSONSavedServerStore(fileURL: storeURL)
        let siteUsageStore = JSONSiteUsageStore(fileURL: usageURL)
        let credentialStore = InMemoryServerCredentialStore()
        let savedServer = ServerProfile(
            id: UUID(),
            name: "Delete Usage",
            endpoint: "delete.example.com",
            port: 22,
            username: "deleter",
            connectionKind: .sftp,
            systemImage: "server.rack",
            accentName: "Orange"
        )

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try savedServerStore.saveServers([savedServer])
        try siteUsageStore.saveUsage([
            savedServer.id: SiteUsageRecord(
                lastConnectedAt: Date(),
                lastConnectionSummary: "deleter@delete.example.com"
            )
        ])

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: savedServerStore,
                siteUsageStore: siteUsageStore,
                credentialStore: credentialStore,
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.deleteServer(savedServer)
            #expect(state.siteUsage(for: savedServer) == nil)
        }

        let reloadedUsage = try siteUsageStore.loadUsage()
        #expect(reloadedUsage[savedServer.id] == nil)

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func saveConnectionDraftCanExplicitlyClearStoredPassword() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let storeURL = baseURL.appendingPathComponent("SavedServers.json")
        let savedServerStore = JSONSavedServerStore(fileURL: storeURL)
        let credentialStore = InMemoryServerCredentialStore()
        let savedServer = ServerProfile(
            id: UUID(),
            name: "Password Reset",
            endpoint: "reset.example.com",
            port: 22,
            username: "reset-user",
            connectionKind: .sftp,
            systemImage: "server.rack",
            accentName: "Orange"
        )

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try savedServerStore.saveServers([savedServer])
        try credentialStore.setPassword("old-password", for: savedServer.id)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: savedServerStore,
                credentialStore: credentialStore,
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            state.selectServer(savedServer)
            #expect(state.connectionDraft.password == "old-password")

            state.clearSavedPasswordFromDraft()
            #expect(state.connectionDraft.password.isEmpty)
            #expect(state.connectionDraft.clearsSavedPassword)

            _ = state.saveConnectionDraftAsSite()
            #expect(state.transferFeedback?.message.contains("Password Reset") == true)
        }

        #expect(try credentialStore.password(for: savedServer.id) == nil)

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func deleteServerRequiresConfirmation() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let storeURL = baseURL.appendingPathComponent("SavedServers.json")
        let savedServerStore = JSONSavedServerStore(fileURL: storeURL)
        let credentialStore = InMemoryServerCredentialStore()
        let savedServer = ServerProfile(
            id: UUID(),
            name: "Confirm Delete",
            endpoint: "confirm.example.com",
            port: 22,
            username: "confirm-user",
            connectionKind: .sftp,
            systemImage: "server.rack",
            accentName: "Orange"
        )

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try savedServerStore.saveServers([savedServer])
        try credentialStore.setPassword("confirm-password", for: savedServer.id)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: savedServerStore,
                credentialStore: credentialStore,
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            state.requestDeleteServer(savedServer)

            #expect(state.deleteServerRequest?.server.id == savedServer.id)
            #expect(state.servers.contains(where: { $0.id == savedServer.id }))

            state.confirmDeleteServerRequest()

            #expect(state.deleteServerRequest == nil)
            #expect(!state.servers.contains(where: { $0.id == savedServer.id }))
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func clearSavedPasswordUpdatesPasswordPresenceState() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let storeURL = baseURL.appendingPathComponent("SavedServers.json")
        let savedServerStore = JSONSavedServerStore(fileURL: storeURL)
        let credentialStore = InMemoryServerCredentialStore()
        let savedServer = ServerProfile(
            id: UUID(),
            name: "Hint State",
            endpoint: "hint.example.com",
            port: 22,
            username: "hint-user",
            connectionKind: .sftp,
            systemImage: "server.rack",
            accentName: "Orange"
        )

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try savedServerStore.saveServers([savedServer])
        try credentialStore.setPassword("hint-password", for: savedServer.id)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: savedServerStore,
                credentialStore: credentialStore,
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            state.selectServer(savedServer)
            #expect(state.hasSavedPasswordForSelectedServer)

            state.clearSavedPasswordFromDraft()

            #expect(!state.hasSavedPasswordForSelectedServer)
            #expect(state.connectionDraft.clearsSavedPassword)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func favoritePlacesPersistAcrossWorkspaceReload() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let favoritesURL = baseURL.appendingPathComponent("FavoritePlaces.json")
        let favoritePlaceStore = JSONFavoritePlaceStore(fileURL: favoritesURL)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)

        let firstState = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                favoritePlaceStore: favoritePlaceStore,
                credentialStore: InMemoryServerCredentialStore(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            firstState.addCurrentLocalDirectoryToFavorites()
            #expect(firstState.favoritePlaces.count == 1)
            #expect(firstState.favoritePlaces.first?.subtitle == localURL.standardizedFileURL.path(percentEncoded: false))
        }

        let secondState = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                favoritePlaceStore: favoritePlaceStore,
                credentialStore: InMemoryServerCredentialStore(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            #expect(secondState.favoritePlaces.count == 1)
            #expect(secondState.favoritePlaces.first?.subtitle == localURL.standardizedFileURL.path(percentEncoded: false))
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func renamedFavoritePersistsAcrossWorkspaceReload() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let favoritesURL = baseURL.appendingPathComponent("FavoritePlaces.json")
        let favoritePlaceStore = JSONFavoritePlaceStore(fileURL: favoritesURL)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)

        let firstState = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                favoritePlaceStore: favoritePlaceStore,
                credentialStore: InMemoryServerCredentialStore(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            firstState.addCurrentLocalDirectoryToFavorites()
            let favorite = try #require(firstState.favoritePlaces.first)
            firstState.beginRenamingFavorite(favorite)
            firstState.favoriteRenameRequest?.proposedName = "Release Assets"
            firstState.submitFavoriteRenameRequest()

            #expect(firstState.favoritePlaces.first?.title == "Release Assets")
        }

        let secondState = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                favoritePlaceStore: favoritePlaceStore,
                credentialStore: InMemoryServerCredentialStore(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            #expect(secondState.favoritePlaces.first?.title == "Release Assets")
            #expect(secondState.favoritePlaces.first?.subtitle == localURL.standardizedFileURL.path(percentEncoded: false))
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func favoriteReorderingPersistsAcrossWorkspaceReload() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let secondFavoriteURL = baseURL.appendingPathComponent("Archives", isDirectory: true)
        let favoritesURL = baseURL.appendingPathComponent("FavoritePlaces.json")
        let favoritePlaceStore = JSONFavoritePlaceStore(fileURL: favoritesURL)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: secondFavoriteURL, withIntermediateDirectories: true)
        try favoritePlaceStore.saveFavoritePlaces([
            FavoritePlaceRecord(url: localURL),
            FavoritePlaceRecord(url: secondFavoriteURL, customTitle: "Archives")
        ])

        let firstState = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                favoritePlaceStore: favoritePlaceStore,
                credentialStore: InMemoryServerCredentialStore(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            #expect(firstState.favoritePlaces.map(\.title) == ["Local", "Archives"])
            firstState.moveFavorite(fromOffsets: IndexSet(integer: 1), toOffset: 0)
            #expect(firstState.favoritePlaces.map(\.title) == ["Archives", "Local"])
        }

        let secondState = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                favoritePlaceStore: favoritePlaceStore,
                credentialStore: InMemoryServerCredentialStore(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            #expect(secondState.favoritePlaces.map(\.title) == ["Archives", "Local"])
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func removingFavoriteUpdatesPersistedPlaces() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let favoritesURL = baseURL.appendingPathComponent("FavoritePlaces.json")
        let favoritePlaceStore = JSONFavoritePlaceStore(fileURL: favoritesURL)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try favoritePlaceStore.saveFavoritePlaces([FavoritePlaceRecord(url: localURL)])

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                favoritePlaceStore: favoritePlaceStore,
                credentialStore: InMemoryServerCredentialStore(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        try await MainActor.run {
            let place = try #require(state.favoritePlaces.first)
            state.removeFavorite(place)
            #expect(state.favoritePlaces.isEmpty)
        }

        let reloadedFavorites = try favoritePlaceStore.loadFavoritePlaces()
        #expect(reloadedFavorites.isEmpty)

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func workspacePreferencesPersistAcrossWorkspaceReload() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let preferencesURL = baseURL.appendingPathComponent("WorkspacePreferences.json")
        let workspacePreferenceStore = JSONWorkspacePreferenceStore(fileURL: preferencesURL)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)

        let firstState = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                workspacePreferenceStore: workspacePreferenceStore,
                credentialStore: InMemoryServerCredentialStore(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            firstState.setShowsInspector(false)
            firstState.setBrowserDensity(.ultraCompact)
            firstState.setMaxConcurrentTransfers(4)
        }

        let secondState = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                workspacePreferenceStore: workspacePreferenceStore,
                credentialStore: InMemoryServerCredentialStore(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            #expect(!secondState.showsInspector)
            #expect(secondState.browserDensity == .ultraCompact)
            #expect(secondState.maxConcurrentTransfers == 4)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func uploadActivityReportsIntermediateProgress() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = localURL.appendingPathComponent("archive.bin")
        let payload = Data(repeating: 0x5A, count: 512 * 1024)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try payload.write(to: sourceFileURL)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                remoteClient: ProgressReportingRemoteClient(rootURL: remoteURL),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.remoteSessionStatus = .connected("deploy@app.example.com")
        }

        let accepted = await MainActor.run {
            state.handleDrop(of: [sourceFileURL], into: .remote)
        }

        try await eventually(timeout: 4) {
            state.recentTransfers.contains(where: {
                $0.title == "archive.bin" &&
                $0.status == .running &&
                $0.progress > 0 &&
                $0.progress < 1
            })
        }

        try await eventually(timeout: 4) {
            state.recentTransfers.contains(where: {
                $0.title == "archive.bin" &&
                $0.status == .completed &&
                $0.progress == 1
            })
        }

        try await MainActor.run {
            #expect(accepted)
            #expect(state.transferFeedback?.message.contains("archive.bin") == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func copyFocusedSelectionUploadsMultipleSelectedLocalItems() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let stateURL = baseURL.appendingPathComponent("State", isDirectory: true)
        let firstURL = localURL.appendingPathComponent("ship-a.txt")
        let secondURL = localURL.appendingPathComponent("ship-b.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: stateURL, withIntermediateDirectories: true)
        try "a".write(to: firstURL, atomically: true, encoding: .utf8)
        try "b".write(to: secondURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                savedServerStore: JSONSavedServerStore(
                    fileURL: stateURL.appendingPathComponent("SavedServers.json", isDirectory: false)
                ),
                favoritePlaceStore: JSONFavoritePlaceStore(
                    fileURL: stateURL.appendingPathComponent("FavoritePlaces.json", isDirectory: false)
                ),
                workspacePreferenceStore: JSONWorkspacePreferenceStore(
                    fileURL: stateURL.appendingPathComponent("WorkspacePreferences.json", isDirectory: false)
                ),
                siteUsageStore: JSONSiteUsageStore(
                    fileURL: stateURL.appendingPathComponent("SiteUsage.json", isDirectory: false)
                ),
                credentialStore: InMemoryServerCredentialStore(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.remoteSessionStatus = .connected("deploy@app.example.com")
            state.focusedPane = .local
            state.selectLocalItems(ids: [
                firstURL.path(percentEncoded: false),
                secondURL.path(percentEncoded: false)
            ])
            state.copyFocusedSelectionToOtherPane()
        }

        try await eventually {
            FileManager.default.fileExists(atPath: remoteURL.appendingPathComponent("ship-a.txt").path) &&
            FileManager.default.fileExists(atPath: remoteURL.appendingPathComponent("ship-b.txt").path) &&
            state.remoteItems.contains(where: { $0.name == "ship-a.txt" }) &&
            state.remoteItems.contains(where: { $0.name == "ship-b.txt" })
        }

        try await MainActor.run {
            #expect(state.remoteItems.contains(where: { $0.name == "ship-a.txt" }))
            #expect(state.remoteItems.contains(where: { $0.name == "ship-b.txt" }))
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func copyFocusedSelectionDownloadsMultipleSelectedRemoteItems() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let firstURL = remoteURL.appendingPathComponent("pull-a.txt")
        let secondURL = remoteURL.appendingPathComponent("pull-b.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "a".write(to: firstURL, atomically: true, encoding: .utf8)
        try "b".write(to: secondURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.focusedPane = .remote
            state.selectRemoteItems(ids: [
                firstURL.path(percentEncoded: false),
                secondURL.path(percentEncoded: false)
            ])
            state.copyFocusedSelectionToOtherPane()
        }

        try await eventually {
            FileManager.default.fileExists(atPath: localURL.appendingPathComponent("pull-a.txt").path) &&
            FileManager.default.fileExists(atPath: localURL.appendingPathComponent("pull-b.txt").path)
        }

        try await MainActor.run {
            #expect(state.localItems.contains(where: { $0.name == "pull-a.txt" }))
            #expect(state.localItems.contains(where: { $0.name == "pull-b.txt" }))
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func uploadConflictRequiresResolutionAndCanOverwrite() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = localURL.appendingPathComponent("same-name.txt")
        let destinationFileURL = remoteURL.appendingPathComponent("same-name.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "local".write(to: sourceFileURL, atomically: true, encoding: .utf8)
        try "remote".write(to: destinationFileURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.remoteSessionStatus = .connected("example@test")
            state.focusedPane = .local
            state.selectLocalItem(id: sourceFileURL.path(percentEncoded: false))
            state.copyFocusedSelectionToOtherPane()
        }

        await MainActor.run {
            #expect(state.transferConflictResolutionRequest?.conflictingNames == ["same-name.txt"])
        }

        await MainActor.run {
            state.resolveTransferConflict(with: .overwrite)
        }

        try await eventually {
            (try? String(contentsOf: destinationFileURL, encoding: .utf8)) == "local"
        }

        try await MainActor.run {
            #expect(state.transferConflictResolutionRequest == nil)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func downloadConflictRequiresResolutionAndCanRename() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let localExistingURL = localURL.appendingPathComponent("same-name.txt")
        let remoteSourceURL = remoteURL.appendingPathComponent("same-name.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "existing".write(to: localExistingURL, atomically: true, encoding: .utf8)
        try "incoming".write(to: remoteSourceURL, atomically: true, encoding: .utf8)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.focusedPane = .remote
            state.selectRemoteItem(id: remoteSourceURL.path(percentEncoded: false))
            state.copyFocusedSelectionToOtherPane()
        }

        await MainActor.run {
            #expect(state.transferConflictResolutionRequest?.conflictingNames == ["same-name.txt"])
            state.resolveTransferConflict(with: .rename)
        }

        let renamedURL = localURL.appendingPathComponent("same-name 2.txt")
        try await eventually {
            FileManager.default.fileExists(atPath: renamedURL.path) &&
            (try? String(contentsOf: renamedURL, encoding: .utf8)) == "incoming"
        }

        try await MainActor.run {
            #expect((try? String(contentsOf: localExistingURL, encoding: .utf8)) == "existing")
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func runningUploadCanBeCancelled() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = localURL.appendingPathComponent("cancel-me.bin")
        let payload = Data(repeating: 0x31, count: 512 * 1024)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try payload.write(to: sourceFileURL)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                remoteClient: ProgressReportingRemoteClient(rootURL: remoteURL),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.remoteSessionStatus = .connected("deploy@app.example.com")
        }

        _ = await MainActor.run {
            state.handleDrop(of: [sourceFileURL], into: .remote)
        }

        let activityID = try await eventuallyActivityID(
            named: "cancel-me.bin",
            matching: { $0.status == .running || $0.status == .queued },
            in: state
        )

        await MainActor.run {
            state.cancelTransferActivity(activityID)
        }

        try await eventually(timeout: 4) {
            state.recentTransfers.contains(where: {
                $0.id == activityID && $0.status == .cancelled
            })
        }

        try await MainActor.run {
            #expect(state.canRetryTransferActivity(activityID))
            #expect(!state.canCancelTransferActivity(activityID))
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func failedUploadCanBeRetried() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = localURL.appendingPathComponent("retry-me.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "retry".write(to: sourceFileURL, atomically: true, encoding: .utf8)

        let flakyClient = FlakyUploadRemoteClient(rootURL: remoteURL)
        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                remoteClient: flakyClient,
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.remoteSessionStatus = .connected("deploy@app.example.com")
        }

        _ = await MainActor.run {
            state.handleDrop(of: [sourceFileURL], into: .remote)
        }

        let activityID = try await eventuallyFailedActivityID(named: "retry-me.txt", in: state)

        await MainActor.run {
            #expect(state.canRetryTransferActivity(activityID))
            state.retryTransferActivity(activityID)
        }

        try await eventually(timeout: 4) {
            state.recentTransfers.contains(where: {
                $0.title == "retry-me.txt" && $0.status == .completed
            })
        }

        try await MainActor.run {
            #expect(state.transferFeedback?.message.contains("retry-me.txt") == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func sftpRemoteLocationNormalizesHostWithScheme() async throws {
        let client = LibraryBackedSFTPRemoteClient(
            config: RemoteConnectionConfig(
                connectionKind: .sftp,
                host: "sftp://example.com/uploads",
                port: 22,
                username: "deploy",
                authenticationMode: .password,
                privateKeyPath: nil,
                publicKeyPath: nil,
                password: "secret",
                addressPreference: .automatic
            )
        )

        let location = client.makeInitialLocation(relativeTo: FileManager.default.temporaryDirectory)

        #expect(location.path == "sftp://example.com/")
    }

    @Test func runningUploadCanBePausedAndResumed() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let sourceFileURL = localURL.appendingPathComponent("pause-me.bin")
        let payload = Data(repeating: 0x41, count: 768 * 1024)

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try payload.write(to: sourceFileURL)

        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                remoteClient: TestRemoteClient(remoteRootURL: remoteURL, uploadChunkDelay: 0.015),
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.remoteSessionStatus = .connected("deploy@app.example.com")
        }

        _ = await MainActor.run {
            state.handleDrop(of: [sourceFileURL], into: .remote)
        }

        let activityID = try await eventuallyActivityID(
            named: "pause-me.bin",
            matching: { $0.status == .running || $0.status == .queued },
            in: state
        )

        await MainActor.run {
            #expect(state.canPauseTransferActivity(activityID))
            state.pauseTransferActivity(activityID)
        }

        try await eventually(timeout: 4) {
            state.recentTransfers.contains(where: {
                $0.id == activityID && $0.status == .paused
            })
        }

        let resumeDeadline = Date().addingTimeInterval(4)
        while Date() < resumeDeadline {
            let canResume = await MainActor.run {
                state.canResumeTransferActivity(activityID)
            }
            if canResume {
                break
            }
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        let canResume = await MainActor.run {
            state.canResumeTransferActivity(activityID)
        }
        #expect(canResume)

        await MainActor.run {
            state.resumeTransferActivity(activityID)
        }

        try await eventually(timeout: 4) {
            state.recentTransfers.contains(where: {
                $0.id == activityID && $0.status == .completed
            }) && FileManager.default.fileExists(atPath: remoteURL.appendingPathComponent("pause-me.bin").path)
        }

        try await MainActor.run {
            #expect(state.transferFeedback?.message.contains("pause-me.bin") == true)
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    @Test func failedUploadBatchCanRetryOnlyFailedItems() async throws {
        let baseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        let localURL = baseURL.appendingPathComponent("Local", isDirectory: true)
        let remoteURL = baseURL.appendingPathComponent("Remote", isDirectory: true)
        let firstURL = localURL.appendingPathComponent("keep.txt")
        let secondURL = localURL.appendingPathComponent("fail.txt")

        try FileManager.default.createDirectory(at: localURL, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: remoteURL, withIntermediateDirectories: true)
        try "keep".write(to: firstURL, atomically: true, encoding: .utf8)
        try "fail".write(to: secondURL, atomically: true, encoding: .utf8)

        let remoteClient = TestRemoteClient(
            remoteRootURL: remoteURL,
            failingUploadNames: ["fail.txt"]
        )
        let state = await MainActor.run {
            TransmitWorkspaceState(
                localFileBrowser: LocalFileBrowserService(),
                remoteClient: remoteClient,
                localFileTransfer: LocalFileTransferService(),
                initialLocalDirectoryURL: localURL,
                initialRemoteDirectoryURL: remoteURL
            )
        }

        await MainActor.run {
            state.remoteSessionStatus = .connected("deploy@app.example.com")
            state.focusedPane = .local
            state.selectLocalItems(ids: [
                firstURL.path(percentEncoded: false),
                secondURL.path(percentEncoded: false)
            ])
            state.copyFocusedSelectionToOtherPane()
        }

        let batchActivityID = try await eventuallyActivityID(
            matching: { activity in
                activity.status == .failed &&
                activity.title != "keep.txt" &&
                activity.title != "fail.txt"
            },
            in: state
        )

        try await eventually(timeout: 4) {
            FileManager.default.fileExists(atPath: remoteURL.appendingPathComponent("keep.txt").path)
        }

        await MainActor.run {
            #expect(state.canRetryTransferActivity(batchActivityID))
            #expect(!FileManager.default.fileExists(atPath: remoteURL.appendingPathComponent("fail.txt").path))
        }

        remoteClient.setFailingUploadNames([])

        await MainActor.run {
            state.retryTransferActivity(batchActivityID)
        }

        try await eventually(timeout: 4) {
            FileManager.default.fileExists(atPath: remoteURL.appendingPathComponent("fail.txt").path)
        }

        try await MainActor.run {
            #expect(state.remoteItems.contains(where: { $0.name == "keep.txt" }))
            #expect(state.remoteItems.contains(where: { $0.name == "fail.txt" }))
        }

        try? FileManager.default.removeItem(at: baseURL)
    }

    private func eventuallyFailedActivityID(named title: String, in state: TransmitWorkspaceState) async throws -> UUID {
        try await eventuallyActivityID(named: title, matching: { $0.status == .failed }, in: state)
    }

    private func eventuallyActivityID(
        matching predicate: @escaping (TransferActivity) -> Bool,
        in state: TransmitWorkspaceState
    ) async throws -> UUID {
        let deadline = Date().addingTimeInterval(4)
        while Date() < deadline {
            if let id = await MainActor.run(body: {
                state.recentTransfers.first(where: predicate)?.id
            }) {
                return id
            }
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        Issue.record("Timed out waiting for transfer activity matching predicate.")
        return UUID()
    }

    private func eventuallyActivityID(
        named title: String,
        matching predicate: @escaping (TransferActivity) -> Bool,
        in state: TransmitWorkspaceState
    ) async throws -> UUID {
        let deadline = Date().addingTimeInterval(4)
        while Date() < deadline {
            if let id = await MainActor.run(body: {
                state.recentTransfers.first(where: { $0.title == title && predicate($0) })?.id
            }) {
                return id
            }
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        Issue.record("Timed out waiting for transfer activity matching \(title).")
        return UUID()
    }
}

private final class Locked<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: Value

    init(_ value: Value) {
        self.storage = value
    }

    var value: Value {
        lock.lock()
        defer { lock.unlock() }
        return storage
    }

    func set(_ value: Value) {
        lock.lock()
        storage = value
        lock.unlock()
    }
}

private struct ProgressReportingRemoteClient: RemoteClient {
    private let wrapped: MockRemoteClient

    init(rootURL: URL) {
        self.wrapped = MockRemoteClient(
            localFileBrowser: LocalFileBrowserService(),
            displayHost: "progress.mock"
        )
    }

    func makeInitialLocation(relativeTo localDirectoryURL: URL) -> RemoteLocation {
        wrapped.makeInitialLocation(relativeTo: localDirectoryURL)
    }

    func makeLocation(for directoryURL: URL) -> RemoteLocation {
        wrapped.makeLocation(for: directoryURL)
    }

    func parentLocation(of location: RemoteLocation) -> RemoteLocation? {
        wrapped.parentLocation(of: location)
    }

    func location(for item: BrowserItem, from currentLocation: RemoteLocation) -> RemoteLocation? {
        wrapped.location(for: item, from: currentLocation)
    }

    func loadDirectorySnapshot(in location: RemoteLocation) throws -> RemoteDirectorySnapshot {
        try wrapped.loadDirectorySnapshot(in: location)
    }

    func destinationDirectoryURL(for location: RemoteLocation) -> URL? {
        wrapped.destinationDirectoryURL(for: location)
    }

    func uploadItem(
        at localURL: URL,
        to remoteLocation: RemoteLocation,
        conflictPolicy: TransferConflictPolicy,
        progress: (@Sendable (TransferProgressSnapshot) -> Void)?,
        isCancelled: (@Sendable () -> Bool)?
    ) throws -> RemoteUploadResult {
        let totalByteCount = try localURL.resourceValues(forKeys: [.fileSizeKey]).fileSize.map(Int64.init) ?? 0
        progress?(.init(completedByteCount: 0, totalByteCount: totalByteCount))
        Thread.sleep(forTimeInterval: 0.12)
        progress?(.init(completedByteCount: totalByteCount / 2, totalByteCount: totalByteCount))
        Thread.sleep(forTimeInterval: 0.12)
        let result = try wrapped.uploadItem(
            at: localURL,
            to: remoteLocation,
            conflictPolicy: conflictPolicy,
            progress: progress,
            isCancelled: isCancelled
        )
        progress?(.init(completedByteCount: totalByteCount, totalByteCount: totalByteCount))
        return result
    }

    func downloadItem(
        named name: String,
        at remotePath: String,
        toDirectory localDirectoryURL: URL,
        localFileTransfer: LocalFileTransferService,
        conflictPolicy: TransferConflictPolicy,
        progress: (@Sendable (TransferProgressSnapshot) -> Void)?,
        isCancelled: (@Sendable () -> Bool)?
    ) throws -> LocalFileTransferResult {
        try wrapped.downloadItem(
            named: name,
            at: remotePath,
            toDirectory: localDirectoryURL,
            localFileTransfer: localFileTransfer,
            conflictPolicy: conflictPolicy,
            progress: progress,
            isCancelled: isCancelled
        )
    }

    func createDirectory(named proposedName: String, in remoteLocation: RemoteLocation) throws -> RemoteMutationResult {
        try wrapped.createDirectory(named: proposedName, in: remoteLocation)
    }

    func renameItem(named originalName: String, at remotePath: String, to proposedName: String) throws -> RemoteMutationResult {
        try wrapped.renameItem(named: originalName, at: remotePath, to: proposedName)
    }

    func deleteItem(named name: String, at remotePath: String, isDirectory: Bool) throws {
        try wrapped.deleteItem(named: name, at: remotePath, isDirectory: isDirectory)
    }
}

private struct FlakyUploadRemoteClient: RemoteClient {
    private let wrapped: MockRemoteClient
    private let didFail = Locked(false)

    init(rootURL: URL) {
        self.wrapped = MockRemoteClient(
            localFileBrowser: LocalFileBrowserService(),
            displayHost: "flaky.mock"
        )
    }

    func makeInitialLocation(relativeTo localDirectoryURL: URL) -> RemoteLocation {
        wrapped.makeInitialLocation(relativeTo: localDirectoryURL)
    }

    func makeLocation(for directoryURL: URL) -> RemoteLocation {
        wrapped.makeLocation(for: directoryURL)
    }

    func parentLocation(of location: RemoteLocation) -> RemoteLocation? {
        wrapped.parentLocation(of: location)
    }

    func location(for item: BrowserItem, from currentLocation: RemoteLocation) -> RemoteLocation? {
        wrapped.location(for: item, from: currentLocation)
    }

    func loadDirectorySnapshot(in location: RemoteLocation) throws -> RemoteDirectorySnapshot {
        try wrapped.loadDirectorySnapshot(in: location)
    }

    func destinationDirectoryURL(for location: RemoteLocation) -> URL? {
        wrapped.destinationDirectoryURL(for: location)
    }

    func uploadItem(
        at localURL: URL,
        to remoteLocation: RemoteLocation,
        conflictPolicy: TransferConflictPolicy,
        progress: (@Sendable (TransferProgressSnapshot) -> Void)?,
        isCancelled: (@Sendable () -> Bool)?
    ) throws -> RemoteUploadResult {
        if !didFail.value {
            didFail.set(true)
            throw NSError(domain: "FlakyUploadRemoteClient", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "Injected upload failure."
            ])
        }

        return try wrapped.uploadItem(
            at: localURL,
            to: remoteLocation,
            conflictPolicy: conflictPolicy,
            progress: progress,
            isCancelled: isCancelled
        )
    }

    func downloadItem(
        named name: String,
        at remotePath: String,
        toDirectory localDirectoryURL: URL,
        localFileTransfer: LocalFileTransferService,
        conflictPolicy: TransferConflictPolicy,
        progress: (@Sendable (TransferProgressSnapshot) -> Void)?,
        isCancelled: (@Sendable () -> Bool)?
    ) throws -> LocalFileTransferResult {
        try wrapped.downloadItem(
            named: name,
            at: remotePath,
            toDirectory: localDirectoryURL,
            localFileTransfer: localFileTransfer,
            conflictPolicy: conflictPolicy,
            progress: progress,
            isCancelled: isCancelled
        )
    }

    func createDirectory(named proposedName: String, in remoteLocation: RemoteLocation) throws -> RemoteMutationResult {
        try wrapped.createDirectory(named: proposedName, in: remoteLocation)
    }

    func renameItem(named originalName: String, at remotePath: String, to proposedName: String) throws -> RemoteMutationResult {
        try wrapped.renameItem(named: originalName, at: remotePath, to: proposedName)
    }

    func deleteItem(named name: String, at remotePath: String, isDirectory: Bool) throws {
        try wrapped.deleteItem(named: name, at: remotePath, isDirectory: isDirectory)
    }
}
