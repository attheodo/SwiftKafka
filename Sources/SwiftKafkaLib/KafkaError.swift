import ckafka

public indirect enum KafkaError: Error {
    
    /// When librdkafka fails to create a new config
    case configCreationError
    
    /// When librdkafka fails to duplicate a config
    case configDuplicationError
    
    /// When librdkafka fail to create a new topic config
    case topicConfigCreationError
    
    /// When librdkafka fails to duplicate a topic config
    case topicConfigDuplicationError
    
    /// When there's an error setting a config variable
    case setConfigurationPropertyError(String)
    
    /// When the request configuration variable was not found
    case configVariableNotFound(String)
    
    /// When a Kafka client handle fails to get created
    case kafkaClientHandleInitFailed(String)
    
    /// Errors emerging from `librdkafka` operations
    case coreError(KafkaCoreError)
    
    /// ¯\_(ツ)_/¯
    case unknownError
}

public struct KafkaCoreError: Error {
    
    // MARK: - Public Properties
    
    public private(set) var rdError: rd_kafka_resp_err_t?
    
    /// The error's enum name
    public var enumName: String {
        
        guard let rdError = self.rdError else {
            return "SWIFTKAFKA_UKNOWN_ERROR"
        }
        
        return String(cString: rd_kafka_err2name(rdError))
        
    }
    
    // MARK: - Overrides
    
    public var localizedDescription: String {
        
        guard let rdError = self.rdError else {
            return "Unknown error"
        }
        
        return String(cString: rd_kafka_err2str(rdError))
        
    }
    
}
