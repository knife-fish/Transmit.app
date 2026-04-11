import Citadel
import Crypto
import Darwin
import Foundation
import NIOCore
import NIOFoundationCompat
import NIOSSH
import Security

enum RemoteClientError: LocalizedError {
    case requestFailed(details: String)
    case operationTimedOut(operation: String, seconds: TimeInterval)

    var errorDescription: String? {
        switch self {
        case .requestFailed(let details):
            return details
        case .operationTimedOut(let operation, let seconds):
            return "\(operation) timed out after \(Int(seconds)) seconds."
        }
    }
}

struct LibraryBackedSFTPRemoteClient: RemoteClient {
    private let config: RemoteConnectionConfig
    private let session: CitadelSFTPSession

    init(config: RemoteConnectionConfig) {
        self.config = config
        self.session = CitadelSFTPSession(config: config)
    }

    func makeInitialLocation(relativeTo localDirectoryURL: URL) -> RemoteLocation {
        makeRemoteLocation(path: "/")
    }

    func makeLocation(for directoryURL: URL) -> RemoteLocation {
        makeRemoteLocation(path: directoryURL.path(percentEncoded: false))
    }

    func parentLocation(of location: RemoteLocation) -> RemoteLocation? {
        guard location.remotePath != "/" else { return nil }
        let parent = (location.remotePath as NSString).deletingLastPathComponent
        return makeRemoteLocation(path: parent.isEmpty ? "/" : parent)
    }

    func location(for item: BrowserItem, from currentLocation: RemoteLocation) -> RemoteLocation? {
        guard item.isDirectory else { return nil }
        return makeRemoteLocation(path: item.pathDescription)
    }

    func loadDirectorySnapshot(in location: RemoteLocation) throws -> RemoteDirectorySnapshot {
        let snapshot = try session.loadDirectorySnapshot(in: location)
        let sortedItems = snapshot.items.sorted { lhs, rhs in
            if lhs.isDirectory != rhs.isDirectory {
                return lhs.isDirectory && !rhs.isDirectory
            }
            return lhs.name.localizedStandardCompare(rhs.name) == .orderedAscending
        }
        return RemoteDirectorySnapshot(
            location: makeRemoteLocation(path: snapshot.location.remotePath),
            items: sortedItems,
            homePath: snapshot.homePath
        )
    }

    func destinationDirectoryURL(for location: RemoteLocation) -> URL? {
        nil
    }

    func uploadItem(
        at localURL: URL,
        to remoteLocation: RemoteLocation,
        conflictPolicy: TransferConflictPolicy,
        progress: (@Sendable (TransferProgressSnapshot) -> Void)?,
        isCancelled: (@Sendable () -> Bool)?
    ) throws -> RemoteUploadResult {
        let resourceValues = try localURL.resourceValues(forKeys: [.isDirectoryKey])
        if resourceValues.isDirectory == true {
            throw CocoaError(.fileReadUnsupportedScheme)
        }

        let destinationName = try resolvedRemoteName(
            proposedName: localURL.lastPathComponent,
            in: remoteLocation,
            conflictPolicy: conflictPolicy
        )
        let destinationPath = join(remotePath: remoteLocation.remotePath, name: destinationName)
        if conflictPolicy == .overwrite,
           let existingItem = try loadItems(in: remoteLocation).first(where: { $0.name == destinationName }) {
            guard !existingItem.isDirectory else {
                throw CocoaError(.fileWriteFileExists)
            }
            try session.deleteItem(at: existingItem.pathDescription, isDirectory: false)
        }
        try session.uploadItem(at: localURL, toPath: destinationPath, progress: progress, isCancelled: isCancelled)
        return RemoteUploadResult(
            remoteItemID: destinationPath,
            destinationName: destinationName,
            renamedForConflict: destinationName != localURL.lastPathComponent
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
        let destinationURL = try localFileTransfer.destinationURL(
            forProposedName: name,
            in: localDirectoryURL,
            conflictPolicy: conflictPolicy
        )
        if conflictPolicy == .overwrite, FileManager.default.fileExists(atPath: destinationURL.path(percentEncoded: false)) {
            try localFileTransfer.deleteItem(at: destinationURL)
        }
        try session.downloadItem(at: remotePath, to: destinationURL, progress: progress, isCancelled: isCancelled)
        return LocalFileTransferResult(
            sourceURL: URL(fileURLWithPath: remotePath),
            destinationURL: destinationURL,
            renamedForConflict: destinationURL.lastPathComponent != name
        )
    }

    func createDirectory(named proposedName: String, in remoteLocation: RemoteLocation) throws -> RemoteMutationResult {
        let sanitizedName = try validateRemoteName(proposedName)
        let destinationName = try makeUniqueRemoteName(proposedName: sanitizedName, in: remoteLocation)
        let destinationPath = join(remotePath: remoteLocation.remotePath, name: destinationName)
        try session.createDirectory(at: destinationPath)
        return RemoteMutationResult(
            remoteItemID: destinationPath,
            destinationName: destinationName
        )
    }

    func renameItem(named originalName: String, at remotePath: String, to proposedName: String) throws -> RemoteMutationResult {
        let sanitizedName = try validateRemoteName(proposedName)
        let parentPath = parentPath(for: remotePath)
        let destinationPath = join(remotePath: parentPath, name: sanitizedName)
        guard destinationPath != remotePath else {
            return RemoteMutationResult(remoteItemID: remotePath, destinationName: sanitizedName)
        }

        let siblingItems = try loadItems(in: makeRemoteLocation(path: parentPath))
        guard !siblingItems.contains(where: { $0.name == sanitizedName && $0.pathDescription != remotePath }) else {
            throw CocoaError(.fileWriteFileExists)
        }

        try session.renameItem(at: remotePath, to: destinationPath)
        return RemoteMutationResult(
            remoteItemID: destinationPath,
            destinationName: sanitizedName
        )
    }

    func deleteItem(named name: String, at remotePath: String, isDirectory: Bool) throws {
        try session.deleteItem(at: remotePath, isDirectory: isDirectory)
    }

    private func makeRemoteLocation(path: String) -> RemoteLocation {
        let normalizedPath = normalizedRemotePath(path)
        return RemoteLocation(
            id: normalizedPath,
            path: "sftp://\(config.normalizedHost)\(normalizedPath)",
            remotePath: normalizedPath,
            directoryURL: nil
        )
    }

    private func normalizedRemotePath(_ path: String) -> String {
        let standardized = NSString(string: path).standardizingPath
        if standardized.isEmpty || standardized == "." {
            return "/"
        }
        return standardized.hasPrefix("/") ? standardized : "/\(standardized)"
    }

    private func join(remotePath: String, name: String) -> String {
        if remotePath == "/" {
            return "/\(name)"
        }
        return "\(remotePath)/\(name)"
    }

    private func parentPath(for remotePath: String) -> String {
        let parent = (remotePath as NSString).deletingLastPathComponent
        if parent.isEmpty {
            return "/"
        }
        return normalizedRemotePath(parent)
    }

    private func validateRemoteName(_ proposedName: String) throws -> String {
        let trimmed = proposedName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, trimmed != ".", trimmed != "..", !trimmed.contains("/") else {
            throw CocoaError(.fileWriteInvalidFileName)
        }
        return trimmed
    }

    private func makeUniqueRemoteName(proposedName: String, in remoteLocation: RemoteLocation) throws -> String {
        let sanitizedName = try validateRemoteName(proposedName)
        let siblingNames = Set(try loadItems(in: remoteLocation).map(\.name))
        guard siblingNames.contains(sanitizedName) else {
            return sanitizedName
        }

        let pathExtension = URL(fileURLWithPath: sanitizedName).pathExtension
        let baseName = pathExtension.isEmpty
            ? sanitizedName
            : String(sanitizedName.dropLast(pathExtension.count + 1))

        var candidateIndex = 2
        while true {
            let candidateBase = "\(baseName) \(candidateIndex)"
            let candidate = pathExtension.isEmpty
                ? candidateBase
                : "\(candidateBase).\(pathExtension)"
            if !siblingNames.contains(candidate) {
                return candidate
            }
            candidateIndex += 1
        }
    }

    private func resolvedRemoteName(
        proposedName: String,
        in remoteLocation: RemoteLocation,
        conflictPolicy: TransferConflictPolicy
    ) throws -> String {
        switch conflictPolicy {
        case .rename:
            return try makeUniqueRemoteName(proposedName: proposedName, in: remoteLocation)
        case .overwrite:
            return try validateRemoteName(proposedName)
        }
    }
}

private final class CitadelSFTPSession: @unchecked Sendable {
    private static let connectionTimeout: TimeInterval = 15
    private static let directoryTimeout: TimeInterval = 15
    private static let mutationTimeout: TimeInterval = 20
    private static let fileTransferTimeout: TimeInterval = 600

    private let config: RemoteConnectionConfig
    private let lock = NSLock()
    private var sshClient: SSHClient?
    private var sftpClient: SFTPClient?

    init(config: RemoteConnectionConfig) {
        self.config = config
    }

    deinit {
        let sshClient = withLock {
            let client = self.sshClient
            self.sshClient = nil
            self.sftpClient = nil
            return client
        }

        guard let sshClient else { return }
        Task.detached {
            try? await sshClient.close()
        }
    }

    func loadDirectorySnapshot(in location: RemoteLocation) throws -> RemoteDirectorySnapshot {
        try withConnectedSFTP(timeout: Self.directoryTimeout, operationDescription: "Loading remote directory") { sftp in
            let homePath = try await sftp.getRealPath(atPath: ".")
            let requestedPath = location.remotePath == "/" ? homePath : location.remotePath
            let targetPath = try await sftp.getRealPath(atPath: requestedPath)

            let entries = try await sftp.listDirectory(atPath: targetPath)
            let items = entries
                .flatMap(\.components)
                .compactMap { self.makeBrowserItem(from: $0, in: targetPath) }

            return RemoteDirectorySnapshot(
                location: RemoteLocation(
                    id: targetPath,
                    path: "sftp://\(self.config.host)\(targetPath)",
                    remotePath: targetPath,
                    directoryURL: nil
                ),
                items: items,
                homePath: homePath
            )
        }
    }

    func uploadItem(
        at localURL: URL,
        toPath remotePath: String,
        progress: (@Sendable (TransferProgressSnapshot) -> Void)?,
        isCancelled: (@Sendable () -> Bool)?
    ) throws {
        let totalByteCount = try localURL.resourceValues(forKeys: [.fileSizeKey]).fileSize.map(Int64.init)
        try withConnectedSFTP(timeout: Self.fileTransferTimeout, operationDescription: "Uploading \(localURL.lastPathComponent)") { sftp in
            let remoteFile = try await sftp.openFile(
                filePath: remotePath,
                flags: [.write, .create, .truncate]
            )
            defer {
                Task.detached {
                    try? await remoteFile.close()
                }
            }

            let localHandle = try FileHandle(forReadingFrom: localURL)
            defer {
                try? localHandle.close()
            }

            let chunkSize = 64 * 1024
            var offset: UInt64 = 0
            var completedByteCount: Int64 = 0
            progress?(.init(completedByteCount: 0, totalByteCount: totalByteCount))

            while true {
                if isCancelled?() == true {
                    throw CancellationError()
                }
                let data = try localHandle.read(upToCount: chunkSize) ?? Data()
                if data.isEmpty { break }

                var buffer = ByteBufferAllocator().buffer(capacity: data.count)
                buffer.writeBytes(data)
                try await remoteFile.write(buffer, at: offset)

                offset += UInt64(data.count)
                completedByteCount += Int64(data.count)
                progress?(.init(completedByteCount: completedByteCount, totalByteCount: totalByteCount))
            }

            if completedByteCount == 0 {
                progress?(.init(completedByteCount: 0, totalByteCount: totalByteCount ?? 0))
            }
        }
    }

    func downloadItem(
        at remotePath: String,
        to destinationURL: URL,
        progress: (@Sendable (TransferProgressSnapshot) -> Void)?,
        isCancelled: (@Sendable () -> Bool)?
    ) throws {
        do {
            try withConnectedSFTP(timeout: Self.fileTransferTimeout, operationDescription: "Downloading \(destinationURL.lastPathComponent)") { sftp in
                let remoteFile = try await sftp.openFile(filePath: remotePath, flags: .read)
                defer {
                    Task.detached {
                        try? await remoteFile.close()
                    }
                }

                let attributes = try await remoteFile.readAttributes()
                let totalByteCount = attributes.size.map(Int64.init)
                FileManager.default.createFile(atPath: destinationURL.path(percentEncoded: false), contents: nil)
                let outputHandle = try FileHandle(forWritingTo: destinationURL)
                defer {
                    try? outputHandle.close()
                }

                let chunkSize: UInt32 = 64 * 1024
                var offset: UInt64 = 0
                var completedByteCount: Int64 = 0
                progress?(.init(completedByteCount: 0, totalByteCount: totalByteCount))

                while true {
                    if isCancelled?() == true {
                        throw CancellationError()
                    }
                    let buffer = try await remoteFile.read(from: offset, length: chunkSize)
                    if buffer.readableBytes == 0 { break }
                    let data = buffer.withUnsafeReadableBytes { Data($0) }
                    try outputHandle.write(contentsOf: data)

                    offset += UInt64(data.count)
                    completedByteCount += Int64(data.count)
                    progress?(.init(completedByteCount: completedByteCount, totalByteCount: totalByteCount))
                }

                if completedByteCount == 0 {
                    progress?(.init(completedByteCount: 0, totalByteCount: totalByteCount ?? 0))
                }
            }
        } catch {
            try? FileManager.default.removeItem(at: destinationURL)
            throw error
        }
    }

    func createDirectory(at remotePath: String) throws {
        try withConnectedSFTP(timeout: Self.mutationTimeout, operationDescription: "Creating remote folder") { sftp in
            try await sftp.createDirectory(atPath: remotePath)
        }
    }

    func renameItem(at remotePath: String, to destinationPath: String) throws {
        try withConnectedSFTP(timeout: Self.mutationTimeout, operationDescription: "Renaming remote item") { sftp in
            try await sftp.rename(at: remotePath, to: destinationPath)
        }
    }

    func deleteItem(at remotePath: String, isDirectory: Bool) throws {
        try withConnectedSFTP(timeout: Self.mutationTimeout, operationDescription: "Deleting remote item") { sftp in
            if isDirectory {
                try await sftp.rmdir(at: remotePath)
            } else {
                try await sftp.remove(at: remotePath)
            }
        }
    }

    private func withConnectedSFTP<T>(
        timeout: TimeInterval,
        operationDescription: String,
        operation: @escaping @Sendable (SFTPClient) async throws -> T
    ) throws -> T {
        try runBlocking(timeout: timeout, operationDescription: operationDescription) {
            let sftp = try await self.connectIfNeeded()
            return try await operation(sftp)
        }
    }

    private func connectIfNeeded() async throws -> SFTPClient {
        if let connectedClient = withLock({ sftpClient }), connectedClient.isActive {
            return connectedClient
        }

        let connectHost: String
        do {
            connectHost = try config.resolvedConnectHost()
        } catch {
            throw normalizedError(error)
        }
        let connectionTimeout = Self.connectionTimeout
        let connectionOperation = "Connecting to \(config.normalizedHost)"
        let authenticationMethod: SSHAuthenticationMethod
        do {
            authenticationMethod = try self.authenticationMethod()
        } catch {
            throw normalizedError(error)
        }

        let settings = SSHClientSettings(
            host: connectHost,
            port: config.port,
            authenticationMethod: { authenticationMethod },
            hostKeyValidator: .acceptAnything()
        )

        do {
            let sshClient = try await withThrowingTaskGroup(of: SSHClient.self) { group in
                group.addTask {
                    try await SSHClient.connect(to: settings)
                }
                group.addTask {
                    try await Task.sleep(for: .seconds(connectionTimeout))
                    throw RemoteClientError.operationTimedOut(
                        operation: connectionOperation,
                        seconds: connectionTimeout
                    )
                }

                guard let firstResult = try await group.next() else {
                    throw RemoteClientError.requestFailed(details: "Citadel did not return a connection result.")
                }
                group.cancelAll()
                return firstResult
            }
            let sftpClient = try await sshClient.openSFTP()
            withLock {
                self.sshClient = sshClient
                self.sftpClient = sftpClient
            }
            return sftpClient
        } catch {
            withLock {
                self.sshClient = nil
                self.sftpClient = nil
            }
            throw normalizedError(error)
        }
    }

    private func runBlocking<T>(
        timeout: TimeInterval,
        operationDescription: String,
        _ operation: @escaping @Sendable () async throws -> T
    ) throws -> T {
        let semaphore = DispatchSemaphore(value: 0)
        let box = BlockingResultBox<T>()
        let task = Task.detached {
            do {
                box.result = Result.success(try await operation())
            } catch {
                box.result = Result.failure(error)
            }
            semaphore.signal()
        }

        let waitResult = semaphore.wait(timeout: .now() + timeout)
        if waitResult == .timedOut {
            task.cancel()
            throw RemoteClientError.operationTimedOut(
                operation: operationDescription,
                seconds: timeout
            )
        }

        guard let result = box.result else {
            throw RemoteClientError.requestFailed(details: "Citadel operation did not produce a result.")
        }
        do {
            return try result.get()
        } catch {
            throw normalizedError(error)
        }
    }

    private func normalizedError(_ error: Error) -> Error {
        if let remoteClientError = error as? RemoteClientError {
            return remoteClientError
        }

        let message = error.localizedDescription.trimmingCharacters(in: .whitespacesAndNewlines)
        if let diagnosticMessage = diagnosticMessage(for: message) {
            return RemoteClientError.requestFailed(details: diagnosticMessage)
        }
        if message.isEmpty || message == "The operation couldn’t be completed." {
            return RemoteClientError.requestFailed(details: "The remote server did not complete the SFTP request.")
        }
        return RemoteClientError.requestFailed(details: message)
    }

    private func diagnosticMessage(for message: String) -> String? {
        let lowercased = message.lowercased()
        let host = config.normalizedHost
        let endpointSummary = "\(host):\(config.port)"

        if config.hostRequiresNormalization {
            return "Invalid host format. Use only the hostname in the Host field, for example \(host), not \(config.host)."
        }

        if lowercased.contains("authentication") || lowercased.contains("auth fail") || lowercased.contains("unable to authenticate") {
            return "SFTP authentication failed for \(config.username)@\(endpointSummary). Verify the selected authentication method, account, and SSH key or password."
        }

        if lowercased.contains("sshclienterror error 4") || lowercased.contains("allauthenticationoptionsfailed") {
            if config.authenticationMode == .sshKey {
                return "SSH key authentication was rejected by \(endpointSummary). Verify that the server accepts this user key, the username matches, and the server allows RSA public key login for this account."
            }
            return "Password authentication was rejected by \(endpointSummary). Verify the username and password."
        }

        if lowercased.contains("sshclienterror error 1") || lowercased.contains("unsupportedprivatekeyauthentication") {
            return "The SSH server at \(endpointSummary) does not accept public key authentication for this session, or it rejected the offered key algorithm."
        }

        if lowercased.contains("connection refused") {
            return "Connection to \(endpointSummary) was refused. Verify that the SSH/SFTP service is listening on that host and port."
        }

        if lowercased.contains("timed out") || lowercased.contains("timeout") {
            return "Connection to \(endpointSummary) timed out. Verify the host, port, network reachability, and any firewall rules."
        }

        if lowercased.contains("no route to host") || lowercased.contains("network is unreachable") {
            return "Cannot reach \(endpointSummary). Verify the host, port, and network route."
        }

        if lowercased.contains("name or service not known") || lowercased.contains("nodename nor servname") || lowercased.contains("unknown host") {
            return "The host \(host) could not be resolved. Check the Host field and DNS."
        }

        if lowercased.contains("disconnected error 1") || lowercased.contains("clienthandshakehandler") {
            return "SSH handshake failed before the SFTP session was established for \(config.username)@\(endpointSummary). Verify the host, port, username, and selected password or SSH key."
        }

        return nil
    }

    private func authenticationMethod() throws -> SSHAuthenticationMethod {
        switch config.authenticationMode {
        case .password:
            let password = config.password ?? ""
            guard !password.isEmpty else {
                throw RemoteClientError.requestFailed(details: "SFTP password is required when password authentication is selected.")
            }
            return SSHAuthenticationMethod.passwordBased(
                username: config.username,
                password: password
            )
        case .sshKey:
            return try loadSSHKeyAuthenticationMethod()
        }
    }

    private func loadSSHKeyAuthenticationMethod() throws -> SSHAuthenticationMethod {
        let trimmedPath = (config.privateKeyPath ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedPath.isEmpty else {
            throw RemoteClientError.requestFailed(details: "Select a private key file for SSH key authentication.")
        }

        let keyURL = URL(fileURLWithPath: NSString(string: trimmedPath).expandingTildeInPath)
        let keyContents = try String(contentsOf: keyURL, encoding: .utf8)
        let passphraseData = config.password?.isEmpty == false ? Data((config.password ?? "").utf8) : nil

        if keyContents.contains("BEGIN OPENSSH PRIVATE KEY") {
            let keyType = try SSHKeyDetection.detectPrivateKeyType(from: keyContents)
            switch keyType {
            case .rsa:
                return .rsa(
                    username: config.username,
                    privateKey: try Insecure.RSA.PrivateKey(sshRsa: keyContents, decryptionKey: passphraseData)
                )
            case .ed25519:
                return .ed25519(
                    username: config.username,
                    privateKey: try Curve25519.Signing.PrivateKey(sshEd25519: keyContents, decryptionKey: passphraseData)
                )
            case .ecdsaP256, .ecdsaP384, .ecdsaP521:
                throw RemoteClientError.requestFailed(details: "OpenSSH ECDSA private keys are not supported by the current SFTP adapter. Export the key as PEM or use an RSA/ED25519 OpenSSH key.")
            default:
                throw RemoteClientError.requestFailed(details: "Unsupported OpenSSH private key type for SFTP authentication.")
            }
        }

        if keyContents.contains("BEGIN EC PRIVATE KEY") || keyContents.contains("BEGIN PRIVATE KEY") {
            if let auth = try pemECDSAAuthenticationMethod(from: keyContents) {
                return auth
            }
        }

        if let auth = try securityRSAPrivateKeyAuthenticationMethod(
            from: keyContents,
            keyURL: keyURL,
            passphrase: config.password
        ) {
            return auth
        }

        throw RemoteClientError.requestFailed(details: "Unsupported private key format. Use a PEM RSA/ECDSA key or an OpenSSH RSA/ED25519 private key.")
    }

    private func pemECDSAAuthenticationMethod(from pem: String) throws -> SSHAuthenticationMethod? {
        if let privateKey = try? P256.Signing.PrivateKey(pemRepresentation: pem) {
            return .p256(username: config.username, privateKey: privateKey)
        }
        if let privateKey = try? P384.Signing.PrivateKey(pemRepresentation: pem) {
            return .p384(username: config.username, privateKey: privateKey)
        }
        if let privateKey = try? P521.Signing.PrivateKey(pemRepresentation: pem) {
            return .p521(username: config.username, privateKey: privateKey)
        }
        return nil
    }

    private func securityRSAPrivateKeyAuthenticationMethod(
        from pem: String,
        keyURL: URL,
        passphrase: String?
    ) throws -> SSHAuthenticationMethod? {
        let privateKey = try importedRSAPrivateKey(from: pem, passphrase: passphrase) ?? {
            guard let derData = pemDERData(from: pem) else {
                return nil
            }

            let attributes: [String: Any] = [
                kSecAttrKeyType as String: kSecAttrKeyTypeRSA,
                kSecAttrKeyClass as String: kSecAttrKeyClassPrivate
            ]
            return SecKeyCreateWithData(derData as CFData, attributes as CFDictionary, nil)
        }()
        guard let privateKey else {
            return nil
        }

        let publicKeyBlob = try resolvedRSAPublicKeyBlob(for: keyURL, privateKey: privateKey)
        let delegate = SecKeyRSAAuthenticationDelegate(
            username: config.username,
            privateKey: privateKey,
            publicKeyBlob: publicKeyBlob
        )
        return .custom(delegate)
    }

    private func importedRSAPrivateKey(from pem: String, passphrase: String?) throws -> SecKey? {
        var externalFormat = SecExternalFormat.formatUnknown
        var itemType = SecExternalItemType.itemTypeUnknown
        var importedItems: CFArray?
        var keyParameters = SecItemImportExportKeyParameters(
            version: UInt32(SEC_KEY_IMPORT_EXPORT_PARAMS_VERSION),
            flags: SecKeyImportExportFlags(rawValue: 0),
            passphrase: nil,
            alertTitle: nil,
            alertPrompt: nil,
            accessRef: nil,
            keyUsage: nil,
            keyAttributes: nil
        )

        let status: OSStatus
        if let passphrase, !passphrase.isEmpty {
            keyParameters.passphrase = Unmanaged.passUnretained(passphrase as CFString)
            status = SecItemImport(
                pem.data(using: .utf8)! as CFData,
                nil,
                &externalFormat,
                &itemType,
                [],
                &keyParameters,
                nil,
                &importedItems
            )
        } else {
            status = SecItemImport(
                pem.data(using: .utf8)! as CFData,
                nil,
                &externalFormat,
                &itemType,
                [],
                nil,
                nil,
                &importedItems
            )
        }

        guard status == errSecSuccess else {
            if status == errSecPassphraseRequired {
                throw RemoteClientError.requestFailed(details: "This RSA private key is encrypted. Enter the key passphrase in the Key Passphrase field and try again.")
            }
            if isRSAPrivateKeyPEM(pem) {
                throw rsaPEMImportError(status: status, passphrase: passphrase)
            }
            return nil
        }

        if let importedKey = (importedItems as? [SecKey])?.first {
            return isRSASecKey(importedKey) ? importedKey : nil
        }
        if let importedDictionary = (importedItems as? [[String: Any]])?.first {
            guard let identity = importedDictionary[kSecImportItemIdentity as String] else {
                return nil
            }
            var importedKey: SecKey?
            let identityStatus = SecIdentityCopyPrivateKey((identity as! SecIdentity), &importedKey)
            guard identityStatus == errSecSuccess, let importedKey else {
                return nil
            }
            return isRSASecKey(importedKey) ? importedKey : nil
        }
        return nil
    }

    private func isRSAPrivateKeyPEM(_ pem: String) -> Bool {
        let trimmed = pem.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.contains("BEGIN RSA PRIVATE KEY")
            || trimmed.contains("BEGIN ENCRYPTED PRIVATE KEY")
    }

    private func rsaPEMImportError(status: OSStatus, passphrase: String?) -> RemoteClientError {
        if status == errSecAuthFailed {
            let details: String
            if let passphrase, !passphrase.isEmpty {
                details = "The Key Passphrase is incorrect for the selected RSA private key."
            } else {
                details = "This RSA private key is encrypted. Enter the key passphrase in the Key Passphrase field and try again."
            }
            return .requestFailed(details: details)
        }

        let statusMessage = (SecCopyErrorMessageString(status, nil) as String?) ?? "Security framework error \(status)."
        return .requestFailed(details: "Failed to import the RSA private key: \(statusMessage)")
    }

    private func isRSASecKey(_ key: SecKey) -> Bool {
        let attributes = SecKeyCopyAttributes(key) as? [String: Any]
        return (attributes?[kSecAttrKeyType as String] as? String) == (kSecAttrKeyTypeRSA as String)
    }

    private func resolvedRSAPublicKeyBlob(for privateKeyURL: URL, privateKey: SecKey) throws -> SSHPublicKeyBlob {
        let candidateURLs = [
            config.publicKeyPath.flatMap { path in
                URL(fileURLWithPath: NSString(string: path).expandingTildeInPath)
            },
            URL(fileURLWithPath: privateKeyURL.path(percentEncoded: false) + ".pub")
        ].compactMap { $0 }

        for candidateURL in candidateURLs {
            guard let contents = try? String(contentsOf: candidateURL, encoding: .utf8) else { continue }
            if let blob = try? SSHPublicKeyBlob(openSSHPublicKey: contents, expectedPrefix: "ssh-rsa") {
                return blob
            }
        }

        if let derivedBlob = try derivedRSAPublicKeyBlob(from: privateKey) {
            return derivedBlob
        }

        throw RemoteClientError.requestFailed(details: "Unable to derive the RSA public key from the PEM private key. If you have the matching .pub file, select it in Public Key and try again.")
    }

    private func derivedRSAPublicKeyBlob(from privateKey: SecKey) throws -> SSHPublicKeyBlob? {
        guard let publicKey = SecKeyCopyPublicKey(privateKey) else {
            return nil
        }

        var error: Unmanaged<CFError>?
        guard let derRepresentation = SecKeyCopyExternalRepresentation(publicKey, &error) as Data? else {
            return nil
        }
        return try SSHPublicKeyBlob(rsaPublicKeyDER: derRepresentation)
    }

    private func pemDERData(from pem: String) -> Data? {
        let trimmed = pem.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.contains("BEGIN "), trimmed.contains("PRIVATE KEY") else {
            return nil
        }

        let base64Payload = trimmed
            .split(separator: "\n")
            .map(String.init)
            .filter {
                !$0.hasPrefix("-----BEGIN")
                    && !$0.hasPrefix("-----END")
                    && !$0.contains(":")
            }
            .joined()
        return Data(base64Encoded: base64Payload)
    }

    private func makeBrowserItem(from component: SFTPPathComponent, in remotePath: String) -> BrowserItem? {
        let name = component.filename.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty, name != ".", name != ".." else { return nil }

        let fullPath = normalizedRemotePath(
            name.hasPrefix("/") ? name : join(remotePath: remotePath, name: name)
        )
        let isDirectory = isDirectory(component)
        let byteCount = component.attributes.size.map(Int64.init)

        return BrowserItem(
            id: fullPath,
            name: (fullPath as NSString).lastPathComponent,
            kind: fileKind(for: name, isDirectory: isDirectory),
            byteCount: isDirectory ? nil : byteCount,
            modifiedAt: component.attributes.accessModificationTime?.modificationTime,
            sizeDescription: isDirectory ? "--" : sizeDescription(byteCount),
            modifiedDescription: modifiedDescription(for: component.attributes.accessModificationTime?.modificationTime),
            pathDescription: fullPath,
            url: nil
        )
    }

    private func isDirectory(_ component: SFTPPathComponent) -> Bool {
        if let permissions = component.attributes.permissions {
            return permissions & 0o170000 == 0o040000
        }
        return component.longname.hasPrefix("d")
    }

    private func fileKind(for name: String, isDirectory: Bool) -> FileKind {
        if isDirectory {
            return .folder
        }

        switch URL(fileURLWithPath: name).pathExtension.lowercased() {
        case "png", "jpg", "jpeg", "gif", "webp", "heic", "tiff", "svg":
            return .image
        case "zip", "tar", "gz", "tgz", "rar", "7z", "xz":
            return .archive
        case "swift", "js", "ts", "json", "sh", "py", "rb", "go", "rs", "yml", "yaml", "toml":
            return .code
        default:
            return .document
        }
    }

    private func sizeDescription(_ byteCount: Int64?) -> String {
        guard let byteCount else { return "--" }
        return ByteCountFormatter.string(fromByteCount: byteCount, countStyle: .file)
    }

    private func modifiedDescription(for date: Date?) -> String {
        guard let date else { return "--" }

        let formatter = RelativeDateTimeFormatter()
        formatter.unitsStyle = .full

        if Calendar.current.isDateInToday(date) {
            let timeFormatter = DateFormatter()
            timeFormatter.timeStyle = .short
            timeFormatter.dateStyle = .none
            return "Today, \(timeFormatter.string(from: date))"
        }

        if Calendar.current.isDateInYesterday(date) {
            return "Yesterday"
        }

        if abs(date.timeIntervalSinceNow) < 7 * 24 * 60 * 60 {
            return formatter.localizedString(for: date, relativeTo: Date()).capitalized
        }

        let dateFormatter = DateFormatter()
        dateFormatter.dateStyle = .medium
        dateFormatter.timeStyle = .none
        return dateFormatter.string(from: date)
    }

    private func join(remotePath: String, name: String) -> String {
        if remotePath == "/" {
            return "/\(name)"
        }
        return "\(remotePath)/\(name)"
    }

    private func normalizedRemotePath(_ path: String) -> String {
        let standardized = NSString(string: path).standardizingPath
        if standardized.isEmpty || standardized == "." {
            return "/"
        }
        return standardized.hasPrefix("/") ? standardized : "/\(standardized)"
    }

    private func withLock<T>(_ operation: () -> T) -> T {
        lock.lock()
        defer { lock.unlock() }
        return operation()
    }
}

private final class BlockingResultBox<Value>: @unchecked Sendable {
    nonisolated(unsafe) var result: Result<Value, Error>?
}

private struct SSHPublicKeyBlob {
    let prefix: String
    let body: Data

    init(prefix: String, body: Data) {
        self.prefix = prefix
        self.body = body
    }

    init(rsaPublicKeyDER: Data) throws {
        let parser = try RSAPKCS1PublicKeyParser(derBytes: rsaPublicKeyDER)
        self.prefix = "ssh-rsa"
        self.body = Self.makeRSABody(
            publicExponent: parser.publicExponent,
            modulus: parser.modulus
        )
    }

    init(openSSHPublicKey: String, expectedPrefix: String) throws {
        let components = openSSHPublicKey
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .split(separator: " ", maxSplits: 2, omittingEmptySubsequences: true)
        guard components.count >= 2 else {
            throw RemoteClientError.requestFailed(details: "The selected public key file is not a valid OpenSSH public key.")
        }

        let prefix = String(components[0])
        guard prefix == expectedPrefix else {
            throw RemoteClientError.requestFailed(details: "The selected public key does not match the private key algorithm \(expectedPrefix).")
        }
        guard let rawData = Data(base64Encoded: String(components[1])) else {
            throw RemoteClientError.requestFailed(details: "The selected public key file could not be decoded.")
        }

        guard let encodedPrefix = Self.readSSHString(from: rawData, offset: 0) else {
            throw RemoteClientError.requestFailed(details: "The selected public key file has an invalid SSH wire format.")
        }
        guard let encodedPrefixString = String(data: encodedPrefix.data, encoding: .utf8),
              encodedPrefixString == prefix
        else {
            throw RemoteClientError.requestFailed(details: "The selected public key file has an invalid SSH wire format.")
        }

        self.prefix = prefix
        self.body = rawData.subdata(in: encodedPrefix.nextOffset..<rawData.count)
    }

    private static func makeRSABody(publicExponent: Data, modulus: Data) -> Data {
        var body = Data()
        body.append(sshMPInt(publicExponent))
        body.append(sshMPInt(modulus))
        return body
    }

    private static func sshMPInt(_ integer: Data) -> Data {
        let normalized = normalizeUnsignedInteger(integer)
        let payload: Data
        if normalized.first.map({ $0 & 0x80 != 0 }) == true {
            payload = Data([0]) + normalized
        } else {
            payload = normalized
        }

        var encoded = Data()
        encoded.append(sshUInt32(UInt32(payload.count)))
        encoded.append(payload)
        return encoded
    }

    private static func normalizeUnsignedInteger(_ integer: Data) -> Data {
        let trimmed = integer.drop { $0 == 0 }
        return trimmed.isEmpty ? Data([0]) : Data(trimmed)
    }

    private static func sshUInt32(_ value: UInt32) -> Data {
        withUnsafeBytes(of: value.bigEndian) { Data($0) }
    }

    private static func readSSHString(from data: Data, offset: Int) -> (data: Data, nextOffset: Int)? {
        guard offset + 4 <= data.count else { return nil }
        let lengthData = data.subdata(in: offset..<(offset + 4))
        let length = lengthData.withUnsafeBytes { rawBuffer -> UInt32 in
            rawBuffer.load(as: UInt32.self).bigEndian
        }
        let start = offset + 4
        let end = start + Int(length)
        guard end <= data.count else { return nil }
        return (data.subdata(in: start..<end), end)
    }
}

private struct RSAPKCS1PublicKeyParser {
    let modulus: Data
    let publicExponent: Data

    init(derBytes: Data) throws {
        var offset = 0
        try Self.consumeTag(0x30, in: derBytes, offset: &offset)
        _ = try Self.readLength(in: derBytes, offset: &offset)
        self.modulus = try Self.readInteger(in: derBytes, offset: &offset)
        self.publicExponent = try Self.readInteger(in: derBytes, offset: &offset)
    }

    private static func readInteger(in data: Data, offset: inout Int) throws -> Data {
        try consumeTag(0x02, in: data, offset: &offset)
        let length = try readLength(in: data, offset: &offset)
        guard offset + length <= data.count else {
            throw RemoteClientError.requestFailed(details: "Failed to parse the derived RSA public key.")
        }
        defer { offset += length }
        return data.subdata(in: offset..<(offset + length))
    }

    private static func consumeTag(_ expected: UInt8, in data: Data, offset: inout Int) throws {
        guard offset < data.count, data[offset] == expected else {
            throw RemoteClientError.requestFailed(details: "Failed to parse the derived RSA public key.")
        }
        offset += 1
    }

    private static func readLength(in data: Data, offset: inout Int) throws -> Int {
        guard offset < data.count else {
            throw RemoteClientError.requestFailed(details: "Failed to parse the derived RSA public key.")
        }

        let firstByte = data[offset]
        offset += 1
        if firstByte & 0x80 == 0 {
            return Int(firstByte)
        }

        let byteCount = Int(firstByte & 0x7f)
        guard byteCount > 0, offset + byteCount <= data.count else {
            throw RemoteClientError.requestFailed(details: "Failed to parse the derived RSA public key.")
        }

        var length = 0
        for _ in 0..<byteCount {
            length = (length << 8) | Int(data[offset])
            offset += 1
        }
        return length
    }
}

private final class SecKeyRSAAuthenticationDelegate: NIOSSHClientUserAuthenticationDelegate {
    private let username: String
    private let privateKeys: [NIOSSHPrivateKey]
    private var nextKeyIndex = 0

    init(username: String, privateKey: SecKey, publicKeyBlob: SSHPublicKeyBlob) {
        self.username = username
        self.privateKeys = [
            NIOSSHPrivateKey(
                custom: SecKeyRSAPrivateKeyRSA512(
                    privateKey: privateKey,
                    publicKey: SecKeyRSAPublicKeyRSA512(blob: publicKeyBlob)
                )
            ),
            NIOSSHPrivateKey(
                custom: SecKeyRSAPrivateKeyRSA256(
                    privateKey: privateKey,
                    publicKey: SecKeyRSAPublicKeyRSA256(blob: publicKeyBlob)
                )
            )
        ]
    }

    func nextAuthenticationType(
        availableMethods: NIOSSHAvailableUserAuthenticationMethods,
        nextChallengePromise: EventLoopPromise<NIOSSHUserAuthenticationOffer?>
    ) {
        guard availableMethods.contains(.publicKey) else {
            nextChallengePromise.fail(SSHClientError.unsupportedPrivateKeyAuthentication)
            return
        }

        guard nextKeyIndex < privateKeys.count else {
            nextChallengePromise.fail(SSHClientError.allAuthenticationOptionsFailed)
            return
        }

        let privateKey = privateKeys[nextKeyIndex]
        nextKeyIndex += 1

        nextChallengePromise.succeed(
            NIOSSHUserAuthenticationOffer(
                username: username,
                serviceName: "",
                offer: .privateKey(.init(privateKey: privateKey))
            )
        )
    }
}

private protocol SecKeyRSAAlgorithmTag {
    static var userAuthAlgorithm: String { get }
    static var signatureAlgorithm: SecKeyAlgorithm { get }
}

private enum SecKeyRSA512Algorithm: SecKeyRSAAlgorithmTag {
    static let userAuthAlgorithm = "rsa-sha2-512"
    static let signatureAlgorithm = SecKeyAlgorithm.rsaSignatureMessagePKCS1v15SHA512
}

private enum SecKeyRSA256Algorithm: SecKeyRSAAlgorithmTag {
    static let userAuthAlgorithm = "rsa-sha2-256"
    static let signatureAlgorithm = SecKeyAlgorithm.rsaSignatureMessagePKCS1v15SHA256
}

private struct SecKeyRSAPublicKey<Algorithm: SecKeyRSAAlgorithmTag>: NIOSSHPublicKeyProtocol {
    static var publicKeyPrefix: String { Algorithm.userAuthAlgorithm }

    let blob: SSHPublicKeyBlob

    var rawRepresentation: Data { blob.body }

    func isValidSignature<D>(_ signature: NIOSSHSignatureProtocol, for data: D) -> Bool where D: DataProtocol {
        false
    }

    func write(to buffer: inout ByteBuffer) -> Int {
        buffer.writeBytes(blob.body)
    }

    static func read(from buffer: inout ByteBuffer) throws -> SecKeyRSAPublicKey {
        guard let data = buffer.readData(length: buffer.readableBytes) else {
            throw RemoteClientError.requestFailed(details: "Failed to decode RSA public key data.")
        }
        return SecKeyRSAPublicKey(blob: SSHPublicKeyBlob(prefix: publicKeyPrefix, body: data))
    }
}

private typealias SecKeyRSAPublicKeyRSA512 = SecKeyRSAPublicKey<SecKeyRSA512Algorithm>
private typealias SecKeyRSAPublicKeyRSA256 = SecKeyRSAPublicKey<SecKeyRSA256Algorithm>

private struct SecKeyRSAPrivateKey<Algorithm: SecKeyRSAAlgorithmTag>: NIOSSHPrivateKeyProtocol {
    static var keyPrefix: String { Algorithm.userAuthAlgorithm }

    let privateKey: SecKey
    let publicKey: NIOSSHPublicKeyProtocol

    func signature<D>(for data: D) throws -> NIOSSHSignatureProtocol where D: DataProtocol {
        let payload = Data(data)
        var error: Unmanaged<CFError>?
        guard let signature = SecKeyCreateSignature(
            privateKey,
            Algorithm.signatureAlgorithm,
            payload as CFData,
            &error
        ) as Data?
        else {
            let details = (error?.takeRetainedValue() as Error?)?.localizedDescription ?? "RSA private key signing failed."
            throw RemoteClientError.requestFailed(details: details)
        }
        return SecKeyRSASignature<Algorithm>(rawRepresentation: signature)
    }
}

private typealias SecKeyRSAPrivateKeyRSA512 = SecKeyRSAPrivateKey<SecKeyRSA512Algorithm>
private typealias SecKeyRSAPrivateKeyRSA256 = SecKeyRSAPrivateKey<SecKeyRSA256Algorithm>

private struct SecKeyRSASignature<Algorithm: SecKeyRSAAlgorithmTag>: NIOSSHSignatureProtocol {
    static var signaturePrefix: String { Algorithm.userAuthAlgorithm }

    let rawRepresentation: Data

    func write(to buffer: inout ByteBuffer) -> Int {
        writeSSHString(rawRepresentation, to: &buffer)
    }

    static func read(from buffer: inout ByteBuffer) throws -> SecKeyRSASignature {
        guard let data = readSSHString(from: &buffer) else {
            throw RemoteClientError.requestFailed(details: "Failed to decode RSA signature data.")
        }
        return SecKeyRSASignature(rawRepresentation: data)
    }
}

@discardableResult
private func writeSSHString(_ data: Data, to buffer: inout ByteBuffer) -> Int {
    buffer.writeInteger(UInt32(data.count))
    return 4 + buffer.writeBytes(data)
}

private func readSSHString(from buffer: inout ByteBuffer) -> Data? {
    guard let length = buffer.readInteger(as: UInt32.self) else { return nil }
    return buffer.readData(length: Int(length))
}

private extension RemoteConnectionConfig {
    func resolvedConnectHost() throws -> String {
        let host = normalizedHost
        guard !host.isEmpty else {
            throw RemoteClientError.requestFailed(details: "The Host field is empty.")
        }

        var hints = addrinfo(
            ai_flags: AI_ADDRCONFIG,
            ai_family: AF_UNSPEC,
            ai_socktype: SOCK_STREAM,
            ai_protocol: IPPROTO_TCP,
            ai_addrlen: 0,
            ai_canonname: nil,
            ai_addr: nil,
            ai_next: nil
        )

        var resultPointer: UnsafeMutablePointer<addrinfo>?
        let status = getaddrinfo(host, String(port), &hints, &resultPointer)
        guard status == 0, let resultPointer else {
            let details = String(cString: gai_strerror(status))
            throw RemoteClientError.requestFailed(
                details: "The host \(host) could not be resolved for SFTP: \(details)."
            )
        }
        defer { freeaddrinfo(resultPointer) }

        let preferred = numericHosts(from: resultPointer, preference: addressPreference)
        guard let connectHost = preferred.first else {
            throw RemoteClientError.requestFailed(
                details: "The host \(host) resolved, but no usable network address was returned for SFTP."
            )
        }
        return connectHost
    }

    var normalizedHost: String {
        let trimmed = host.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.contains("://") else { return trimmed }

        if
            let components = URLComponents(string: trimmed),
            let normalizedHost = components.host,
            !normalizedHost.isEmpty
        {
            return normalizedHost
        }

        let withoutScheme = trimmed.replacingOccurrences(
            of: #"^[A-Za-z][A-Za-z0-9+\-.]*://"#,
            with: "",
            options: .regularExpression
        )
        let hostPortAndPath = withoutScheme.split(separator: "/", maxSplits: 1, omittingEmptySubsequences: false).first.map(String.init) ?? withoutScheme
        let hostOnly = hostPortAndPath.split(separator: "@").last.map(String.init) ?? hostPortAndPath
        return hostOnly.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false).first.map(String.init) ?? trimmed
    }

    var hostRequiresNormalization: Bool {
        normalizedHost != host.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    var urlScheme: String {
        switch connectionKind {
        case .sftp:
            return "sftp"
        case .webdav:
            return "dav"
        case .cloud:
            return "cloud"
        }
    }
}

private func numericHosts(
    from head: UnsafeMutablePointer<addrinfo>,
    preference: ConnectionAddressPreference
) -> [String] {
    var ipv4Hosts: [String] = []
    var ipv6Hosts: [String] = []
    var cursor: UnsafeMutablePointer<addrinfo>? = head

    while let entry = cursor?.pointee {
        defer { cursor = entry.ai_next }

        let family = entry.ai_family
        guard family == AF_INET || family == AF_INET6 else { continue }
        guard let address = entry.ai_addr else { continue }

        var hostBuffer = [CChar](repeating: 0, count: Int(NI_MAXHOST))
        let status = getnameinfo(
            address,
            entry.ai_addrlen,
            &hostBuffer,
            socklen_t(hostBuffer.count),
            nil,
            0,
            NI_NUMERICHOST
        )
        guard status == 0 else { continue }

        let host = String(cString: hostBuffer)
        if family == AF_INET {
            ipv4Hosts.append(host)
        } else {
            ipv6Hosts.append(host)
        }
    }

    let orderedHosts: [String]
    switch preference {
    case .automatic, .ipv4:
        orderedHosts = ipv4Hosts + ipv6Hosts
    case .ipv6:
        orderedHosts = ipv6Hosts + ipv4Hosts
    }

    return Array(NSOrderedSet(array: orderedHosts)) as? [String] ?? orderedHosts
}
