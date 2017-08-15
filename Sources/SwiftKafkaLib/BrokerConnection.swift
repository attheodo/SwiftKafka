/// An enum representing the protocol used for a broker connection
public enum BrokerConnectionProtocol: String {
    
    case plaintext = "PLAINTEXT://"
    case ssl = "SSL://"
    case sasl = "SASL://"
    case sasl_plaintext = "SASL_PLAINTEXT://"
    
}

public struct BrokerConnection {
    
    // MARK: - Private Properties
    
    /// The used protocol for this broker connection
    fileprivate let `protocol`: BrokerConnectionProtocol
    
    /// The port of this broker connection
    fileprivate let port: Int
    
    /// The host of this broker connection
    fileprivate let host: String
    
    // MARK: - Initialiser
    init(withProtocol protocol: BrokerConnectionProtocol, host: String, port: Int) {
        
        self.`protocol` = `protocol`
        self.host = host
        self.port = port
        
    }
    
}

extension BrokerConnection: CustomStringConvertible {
    
    public var description: String {
        return "\(`protocol`.rawValue)\(host):\(port)"
    }
    
}
