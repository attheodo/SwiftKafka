//
//  KafkaBase.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 26/08/2017.
//
//

import ckafka

let kSwiftKafkaCStringSize = 1024

/// Enum representing the possible Kafka
/// client types
public enum KafkaClientType {
    
    case consumer
    case producer
    
    func toRawType() -> rd_kafka_type_t {
        
        switch self {
        
        case .consumer:
            return RD_KAFKA_CONSUMER
        case .producer:
            return RD_KAFKA_PRODUCER
        
        }
        
    }
    
}

public class KafkaBase {
    
    // MARK: - Static
    
    /**
     Returns `librdkafka` version as a string
     */
    public static func kafkaVersion() -> String {
        return String(cString: rd_kafka_version_str())
    }
    
    // MARK: - Public Properties
    
    /// The current Kafka client type
    public let clientType: KafkaClientType
    
    /// The Kafka client name
    public var name: String? {
        
        guard let handle = handle, let name = rd_kafka_name(handle) else {
            return nil
        }
        
        return String(cString: name)
        
    }
    
    /// The underlying librdkafka C pointer handle
    public private(set) var handle: OpaquePointer?
    
    /// The currently set client configuration
    public private(set) var globalConfiguration: GlobalConfig?
    
    /// The currently set topic configuration
    public private(set) var topicConfiguration: TopicConfig?
    
    // MARK: - Initialiser
    
    public init(withClientType clientType: KafkaClientType,
                globalConfig: GlobalConfig? = nil,
                andTopicConfig topicConfig: TopicConfig? = nil) throws
    {
        
        if globalConfig == nil {
            globalConfiguration = try GlobalConfig()
        } else {
            globalConfiguration = globalConfig
        }
        
        if topicConfig == nil {
            topicConfiguration = try TopicConfig()
        } else {
            topicConfiguration = topicConfig
        }
        
        let errString = UnsafeMutablePointer<CChar>.allocate(capacity: kSwiftKafkaCStringSize)
        
        defer {
            errString.deallocate(capacity: kSwiftKafkaCStringSize)
        }
        
        guard let handle = rd_kafka_new(clientType.toRawType(),
                                                   globalConfiguration!.handle,
                                                   errString,
                                                   kSwiftKafkaCStringSize) else
        {
            
            rd_kafka_topic_conf_destroy(topicConfiguration?.handle)
            rd_kafka_conf_destroy(globalConfiguration?.handle)
            
            throw KafkaError.baseKafkaInitFailed(String(cString: errString))
            
        }
        
        self.handle = handle
        self.clientType = clientType
        
    }
    
    deinit {
        
        guard let h = self.handle else {
            return
        }
        
        rd_kafka_destroy(h)
        
    }
    
    // MARK: - Public Methods
    /**
     Adds one or more brokers to the kafka handle's list of initial bootstrap brokers.
     - Additional brokers will be discovered automatically as soon as rdkafka connects to a broker by querying the broker metadata.
     - If a broker name resolves to multiple addresses (and possibly address families) all will be used for connection attempts in round-robin fashion.
     - parameter brokers: A list of bootstrap brokers to connect to
     - returns: The number of brokers that were successfully added
     */
    @discardableResult
    public func connect(toBrokers brokers: [Broker]) -> Int {
        
        let brokersList = brokers.map({ $0.description }).joined(separator: ",")
        return Int(rd_kafka_brokers_add(handle, brokersList))
        
    }
    
}
