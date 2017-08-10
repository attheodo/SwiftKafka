import ckafka

public struct KafkaConfig {
    
    // MARK: - Private Properties
    private var configHandle: OpaquePointer
    
    // MARK: - Initialiser
    init?() {
        
        guard let config = rd_kafka_conf_new() else {
            return nil
        }
        
        configHandle = config
        
    }
    
}
