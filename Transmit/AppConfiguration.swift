import Foundation

enum AppConfiguration {
    private enum InfoKey: String {
        case identifierNamespace = "TransmitIdentifierNamespace"
        case keychainService = "TransmitKeychainService"
        case remoteDragItemTypeIdentifier = "TransmitRemoteDragItemTypeIdentifier"
        case remoteDragTextPrefix = "TransmitRemoteDragTextPrefix"
        case remoteDragCollectionTextPrefix = "TransmitRemoteDragCollectionTextPrefix"
        case localDragItemTypeIdentifier = "TransmitLocalDragItemTypeIdentifier"
    }

    private static let fallbackNamespace = Bundle.main.bundleIdentifier?.lowercased() ?? "org.knifefish.transmit"
    private static let fallbackKeychainService = "\(Bundle.main.bundleIdentifier ?? "org.knifefish.Transmit").saved-sites"

    static let identifierNamespace = requiredString(for: .identifierNamespace, fallback: fallbackNamespace)
    static let keychainService = requiredString(for: .keychainService, fallback: fallbackKeychainService)
    static let remoteDragItemTypeIdentifier = requiredString(
        for: .remoteDragItemTypeIdentifier,
        fallback: "\(fallbackNamespace).remote-item"
    )
    static let remoteDragTextPrefix = requiredString(
        for: .remoteDragTextPrefix,
        fallback: "\(fallbackNamespace).remote-item:"
    )
    static let remoteDragCollectionTextPrefix = requiredString(
        for: .remoteDragCollectionTextPrefix,
        fallback: "\(fallbackNamespace).remote-items:"
    )
    static let localDragItemTypeIdentifier = requiredString(
        for: .localDragItemTypeIdentifier,
        fallback: "\(fallbackNamespace).local-file-items"
    )

    static func validate() {
        _ = identifierNamespace
        _ = keychainService
        _ = remoteDragItemTypeIdentifier
        _ = remoteDragTextPrefix
        _ = remoteDragCollectionTextPrefix
        _ = localDragItemTypeIdentifier
    }

    static func queueLabel(_ suffix: String) -> String {
        "\(identifierNamespace).\(suffix)"
    }

    private static func requiredString(for key: InfoKey, fallback: String) -> String {
        guard let value = Bundle.main.object(forInfoDictionaryKey: key.rawValue) as? String, !value.isEmpty else {
            assertionFailure("Missing Info.plist key: \(key.rawValue)")
            return fallback
        }
        return value
    }
}
