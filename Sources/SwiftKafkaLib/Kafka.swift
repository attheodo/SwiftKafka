import ckafka

let kSwiftKafkaCStringSize = 1024

/// Enum representing the possible Kafka
/// client types
public enum KafkaClientType {
    
    case consumer
    case producer
    
    func toLibRdType() -> rd_kafka_type_t {
        
        switch self {
        
        case .consumer:
            return RD_KAFKA_CONSUMER
        
        case .producer:
            return RD_KAFKA_PRODUCER
        }
        
    }
    
}

public class Kafka {
    
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
        
        guard let handle = kafkaClientHandle, let name = rd_kafka_name(handle) else {
            return nil
        }
        
        return String(cString: name)
        
    }
    
    /// The underlying librdkafka C pointer handle
    public private(set) var kafkaClientHandle: OpaquePointer?
    
    /// The currently set client configuration
    public private(set) var configuration: KafkaConfig?
    
    /// The currently set topic configuration
    public private(set) var topicConfiguration: TopicConfig?
    
    // MARK: - Initialiser
    public init(withClientType clientType: KafkaClientType,
                config: KafkaConfig? = nil,
                andTopicConfig topicConfig: TopicConfig? = nil) throws
    {
        
        if config == nil {
            configuration = try KafkaConfig()
        } else {
            configuration = config
        }
        
        let errString = UnsafeMutablePointer<CChar>.allocate(capacity: kSwiftKafkaCStringSize)
        
        defer {
            errString.deallocate(capacity: kSwiftKafkaCStringSize)
        }
        
        guard let kafkaClientHandle = rd_kafka_new(clientType.toLibRdType(),
                                                   configuration?.configHandle,
                                                   errString,
                                                   kSwiftKafkaCStringSize) else
        {
            throw KafkaError.kafkaClientHandleInitFailed(String(cString: errString))
        }
        
        self.kafkaClientHandle = kafkaClientHandle
        self.clientType = clientType
        
    }
    
    deinit {
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            return
        }
        
        rd_kafka_destroy(kafkaClientHandle)
        
    }
    
    // MARK: - Public Methods
    
    /**
     Adds one or more brokers to the kafka handle's list of initial bootstrap brokers.
     - Additional brokers will be discovered automatically as soon as rdkafka connects to a broker by querying the broker metadata.
     - If a broker name resolves to multiple addresses (and possibly address families) all will be used for connection attempts in round-robin fashion.
     - parameter brokers: A list of bootstrap brokers to connect to
     - returns: The number of brokers that were successfully added
    */
    public func connect(toBrokers brokers: [BrokerConnection]) -> Int {
        
        let brokersList = brokers.map({ $0.description }).joined(separator: ",")
        return Int(rd_kafka_brokers_add(kafkaClientHandle, brokersList))
        
    }
    
    public func queryWatermarkOffsets(forTopic topic: String,
                                      partition: Int32 = RD_KAFKA_PARTITION_UA,
                                      timeout: Int32 = 1000,
                                      cached: Bool = false) throws -> (low: Int64, high: Int64)
    {
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            throw KafkaError.unknownError
        }
        
        let low = UnsafeMutablePointer<Int64>.allocate(capacity: 1)
        let high = UnsafeMutablePointer<Int64>.allocate(capacity: 1)
        
        defer {
            
            low.deallocate(capacity: 1)
            high.deallocate(capacity: 1)
        
        }
        
        var response: rd_kafka_resp_err_t
        
        if cached {
            response = rd_kafka_get_watermark_offsets(kafkaClientHandle,
                                                      topic,
                                                      partition,
                                                      low,
                                                      high)
        } else {
            response = rd_kafka_query_watermark_offsets(kafkaClientHandle,
                                                        topic,
                                                        partition,
                                                        low,
                                                        high,
                                                        timeout)
        }
        
        guard response == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(rdError: response))
        }
        
        return (low: low.pointee, high: high.pointee)
        
    }
    
    // MARK: - Interal Methods
    internal func getMetadata(forTopicHandle topicHandle: OpaquePointer? = nil,
                              timeout: Int32 = 1000) throws -> Metadata
    {
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            throw KafkaError.unknownError
        }
        
        let ppMetadata = UnsafeMutablePointer<UnsafePointer<rd_kafka_metadata>?>.allocate(capacity: 1)
        var error: rd_kafka_resp_err_t
        
        if let topicHandle = topicHandle {
            error = rd_kafka_metadata(kafkaClientHandle, 0, topicHandle, ppMetadata, timeout)
        } else {
            error = rd_kafka_metadata(kafkaClientHandle, 1, nil, ppMetadata, timeout)
        }
        
        guard error.rawValue == 0 else {
            let coreError = KafkaCoreError(rdError: error)
            throw KafkaError.coreError(coreError)
        }
        
        guard let rawMetadata = (ppMetadata.pointee)?.pointee else {
            throw KafkaError.unknownError
        }
        
        rd_kafka_metadata_destroy(ppMetadata.pointee)
        ppMetadata.deallocate(capacity: 1)
        
        return Metadata.metadata(fromRawMetadata: rawMetadata)
        
    }
    
}
