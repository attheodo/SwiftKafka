import ckafka

public struct KafkaMessage {
    
    // MARK: - Static
    public static func message(fromRawMessage message: rd_kafka_message_t) -> KafkaMessage {
        
        var error: KafkaError? = nil
        
        if message.err != RD_KAFKA_RESP_ERR_NO_ERROR {
            error = KafkaError.coreError(KafkaCoreError(rdError: message.err))
        }
        
        return KafkaMessage(partition: message.partition,
                            offset: message.offset,
                            error: error)
        
    }
    
    // MARK: - Public Properties
    
    /// The partition of the message
    public var partition: Int32 = 0
    
    /// The offset of the message inside the topic
    public var offset: Int64 = -1
    
    /// Whether the message has an error or not
    public var error: KafkaError? = nil
    
}
