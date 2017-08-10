import ckafka

/// Enum representing the possible Kafka
/// client types
public enum KafkaClientType {
    
    case consumer
    case producer
    
    func rdType() -> rd_kafka_type_t {
        
        switch self {
        
        case .consumer:
            return RD_KAFKA_CONSUMER
        
        case .producer:
            return RD_KAFKA_PRODUCER
        }
        
    }
    
}

public class SwiftKafka {
    
    // MARK: - Static
    
    /**
     Returns `librdkafka` version as a string
    */
    public static func kafkaVersion() -> String {
        return String(cString: rd_kafka_version_str())
    }
    
    // MARK: - Private Properties
    public private(set) var kafkaClientHandle: OpaquePointer?
    
    // MARK: - Initialiser
    public init(withClientType clientType: KafkaClientType,
                andConfig config: KafkaConfig? = nil) throws
    {
        
    }
    
    deinit {
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            return
        }
        
        rd_kafka_destroy(kafkaClientHandle)
        
    }
    
}
