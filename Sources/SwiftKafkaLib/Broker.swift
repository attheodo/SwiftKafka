//
//  Broker.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 26/08/2017.
//
//

/// An enum representing the protocol used for a broker connection
public enum BrokerProtocol: String {
    
    case plaintext = "PLAINTEXT://"
    case ssl = "SSL://"
    case sasl = "SASL://"
    case sasl_plaintext = "SASL_PLAINTEXT://"
    
}

/// Represents a Kafka broker that we can connect to
public struct Broker {
    
    // MARK: - Private Properties
    
    /// The used protocol for this broker connection
    fileprivate let `protocol`: BrokerProtocol
    
    /// The port of this broker connection
    fileprivate let port: Int
    
    /// The host of this broker connection
    fileprivate let host: String
    
    // MARK: - Initialiser
    
    init(withProtocol protocol: BrokerProtocol = .plaintext, host: String, port: Int) {
        
        self.`protocol` = `protocol`
        self.host = host
        self.port = port
        
    }
    
}
