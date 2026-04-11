import Foundation

struct LocalFileBrowserService {
    private let metadataKeys: Set<URLResourceKey> = [
        .isDirectoryKey,
        .contentModificationDateKey,
        .fileSizeKey,
        .isRegularFileKey,
        .nameKey
    ]

    func makeInitialDirectoryURL() -> URL {
        let fileManager = FileManager.default
        let downloadsURL: URL? = fileManager.urls(for: .downloadsDirectory, in: .userDomainMask).first
        let documentsURL: URL? = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first
        let desktopURL: URL? = fileManager.urls(for: .desktopDirectory, in: .userDomainMask).first
        let temporaryURL: URL? = fileManager.temporaryDirectory

        let candidates = [downloadsURL, documentsURL, desktopURL, temporaryURL].compactMap { $0 }
        var firstNonEmptyCandidate: URL?

        for candidate in candidates {
            guard let contents = try? fileManager.contentsOfDirectory(
                at: candidate,
                includingPropertiesForKeys: [.isDirectoryKey],
                options: [.skipsHiddenFiles]
            ) else {
                continue
            }

            if !contents.isEmpty, firstNonEmptyCandidate == nil {
                firstNonEmptyCandidate = candidate
            }

            let containsDirectory = contents.contains { url in
                (try? url.resourceValues(forKeys: [.isDirectoryKey]).isDirectory) == true
            }

            if containsDirectory {
                return candidate
            }
        }

        if let firstNonEmptyCandidate {
            return firstNonEmptyCandidate
        }

        return ensureFallbackWorkspace()
    }

    func makeSecondaryDirectoryURL(relativeTo primaryDirectoryURL: URL) -> URL {
        let fileManager = FileManager.default
        let downloadsURL: URL? = fileManager.urls(for: .downloadsDirectory, in: .userDomainMask).first
        let documentsURL: URL? = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first
        let desktopURL: URL? = fileManager.urls(for: .desktopDirectory, in: .userDomainMask).first
        let temporaryURL: URL? = fileManager.temporaryDirectory

        let candidates = [documentsURL, downloadsURL, desktopURL, temporaryURL]
            .compactMap { $0?.standardizedFileURL }
        let primary = primaryDirectoryURL.standardizedFileURL

        for candidate in candidates where candidate != primary {
            if let contents = try? fileManager.contentsOfDirectory(
                at: candidate,
                includingPropertiesForKeys: [.isDirectoryKey],
                options: [.skipsHiddenFiles]
            ), !contents.isEmpty {
                return candidate
            }
        }

        let parent = primary.deletingLastPathComponent()
        if parent != primary {
            return parent
        }

        return ensureFallbackWorkspace()
    }

    func loadItems(in directoryURL: URL) throws -> [BrowserItem] {
        let fileManager = FileManager.default
        let urls = try fileManager.contentsOfDirectory(
            at: directoryURL,
            includingPropertiesForKeys: Array(metadataKeys),
            options: [.skipsHiddenFiles]
        )

        let items = urls.compactMap(makeBrowserItem(from:))

        return items.sorted { lhs, rhs in
            if lhs.isDirectory != rhs.isDirectory {
                return lhs.isDirectory && !rhs.isDirectory
            }
            return lhs.name.localizedStandardCompare(rhs.name) == .orderedAscending
        }
    }

    private func makeBrowserItem(from url: URL) -> BrowserItem? {
        guard let values = try? url.resourceValues(forKeys: metadataKeys) else {
            return nil
        }

        let isDirectory = values.isDirectory ?? false
        let name = values.name ?? url.lastPathComponent

        return BrowserItem(
            id: url.standardizedFileURL.path(percentEncoded: false),
            name: name,
            kind: fileKind(for: url, isDirectory: isDirectory),
            byteCount: isDirectory ? nil : Int64(values.fileSize ?? 0),
            modifiedAt: values.contentModificationDate,
            sizeDescription: sizeDescription(for: values, isDirectory: isDirectory),
            modifiedDescription: modifiedDescription(for: values.contentModificationDate),
            pathDescription: url.path(percentEncoded: false),
            url: url
        )
    }

    private func fileKind(for url: URL, isDirectory: Bool) -> FileKind {
        if isDirectory {
            return .folder
        }

        switch url.pathExtension.lowercased() {
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

    private func sizeDescription(for values: URLResourceValues, isDirectory: Bool) -> String {
        guard !isDirectory else { return "--" }
        guard let fileSize = values.fileSize else { return "--" }
        return ByteCountFormatter.string(fromByteCount: Int64(fileSize), countStyle: .file)
    }

    private func modifiedDescription(for date: Date?) -> String {
        guard let date else { return "--" }

        let formatter = RelativeDateTimeFormatter()
        formatter.unitsStyle = .full

        let relative = formatter.localizedString(for: date, relativeTo: Date())
        if Calendar.current.isDateInToday(date) {
            let timeFormatter = DateFormatter()
            timeFormatter.timeStyle = .short
            timeFormatter.dateStyle = .none
            return "\(String(localized: "Today")), \(timeFormatter.string(from: date))"
        }

        if Calendar.current.isDateInYesterday(date) {
            return String(localized: "Yesterday")
        }

        if abs(date.timeIntervalSinceNow) < 7 * 24 * 60 * 60 {
            return relative.capitalized
        }

        let formatter2 = DateFormatter()
        formatter2.dateStyle = .medium
        formatter2.timeStyle = .none
        return formatter2.string(from: date)
    }

    private func ensureFallbackWorkspace() -> URL {
        let fileManager = FileManager.default
        let baseURL = fileManager.temporaryDirectory
            .appendingPathComponent("TransmitLocalWorkspace", isDirectory: true)

        if !fileManager.fileExists(atPath: baseURL.path) {
            try? fileManager.createDirectory(at: baseURL, withIntermediateDirectories: true)
            try? fileManager.createDirectory(at: baseURL.appendingPathComponent("Incoming", isDirectory: true), withIntermediateDirectories: true)
            try? fileManager.createDirectory(at: baseURL.appendingPathComponent("Archives", isDirectory: true), withIntermediateDirectories: true)
            try? String(localized: "Welcome to Transmit local browsing.\n").write(
                to: baseURL.appendingPathComponent("README.txt"),
                atomically: true,
                encoding: .utf8
            )
        }

        return baseURL
    }
}
