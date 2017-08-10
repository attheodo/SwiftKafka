import ckafka

public indirect enum SwiftKafkaError: Error {
    
    /// When librdkafka fails to create a new config
    case configCreationError
    
    /// When librdkafka fails to duplicate a config
    case configDuplicationError
    
    /// When there's an error setting a config variable
    case setConfigurationPropertyError(String)
    
    /// When the request configuration variable was not found
    case configVariableNotFound(String)
    
    /// When a Kafka client handle fails to get created
    case kafkaClientHandleInitFailed(String)
    
    /// Errors emerging from `librdkafka` operations
    case coreError(SwiftKafkaCoreError)
    
    /// ¯\_(ツ)_/¯
    case unknownError
}

public struct SwiftKafkaCoreError: Error {
    
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
