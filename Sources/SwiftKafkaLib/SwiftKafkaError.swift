import ckafka

public struct KafkaError: Error {
    
    // MARK: - Public Properties
    
    public var rdError: rd_kafka_resp_err_t?
    
    // MARK: - Overrides
    
    public var localizedDescription: String {
        
        guard let rdError = self.rdError else {
            return "Unknown error"
        }
        
        return String(cString: rd_kafka_err2str(rdError))
        
    }
    
}
