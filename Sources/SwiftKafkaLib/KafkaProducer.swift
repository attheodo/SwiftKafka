import Foundation
import ckafka

class KafkaProducer: SwiftKafka {
    
    // MARK: - Public Properties
    
    public var messageDeliveredClosure: ((KafkaMessage) -> Void)?
    
    /// The number of messages waiting to be delivered
    /// to the broker
    public var numOfPendingMessages: Int32 {
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            return 0
        }
        
        return rd_kafka_outq_len(kafkaClientHandle)
    
    }
    
    /// The underlying librdkafka C pointer handle for the topic
    public private(set) var topicHandle: OpaquePointer? = nil
    
    /// The name of the producing topic
    public let topic: String
    
    // MARK: - Private Properties
    
    // MARK: - Initialiser
    public init(withTopicName topic: String,
                topicConfig: TopicConfig? = nil,
                kafkaConfig: KafkaConfig? = nil) throws
    {
        
        self.topic = topic
        
        let kafkaConfig = try (kafkaConfig ?? (try KafkaConfig()))
        
        // Configure message delivery callback
        rd_kafka_conf_set_dr_msg_cb(kafkaConfig.configHandle) { handler, message, _ in
            
            guard let rawMessage = message else {
                return
            }
           
            let message = KafkaMessage.message(fromRawMessage: rawMessage.pointee,
                                               topic: String(cString: rd_kafka_topic_name(rawMessage.pointee.rkt)))
            
            if let producerRef = (rawMessage.pointee)._private {
                
                let producerInstance = Unmanaged<KafkaProducer>.fromOpaque(producerRef).takeUnretainedValue()
                producerInstance.messageDeliveredClosure?(message)
                
            }
            
        }
        
        try super.init(withClientType: .producer, andConfig: kafkaConfig)
        try createKafkaTopic(withTopicConfig: topicConfig)
        
        KafkaProducer.clientInstances[kafkaClientHandle!] = self
        
    }
    
    deinit {
        
        // wait for the queue to be flushed
        flush(1000)
        
        if let topicHandle = self.topicHandle {
            rd_kafka_topic_destroy(topicHandle)
        }
        
        if let kafkaClientHandle = self.kafkaClientHandle {
            KafkaProducer.clientInstances[kafkaClientHandle] = nil
        }
        
        
    }
    
    // MARK: - Public Methods
    
    /**
     Wait for all messages in the producer queue to be delivered
    */
    public func flush(_ timeout: UInt = 100) {
        
        while(numOfPendingMessages > 0) {
            let _ = try? poll(timeout: Int32(timeout))
        }
        
    }
    
    @discardableResult
    public func poll(timeout: Int32 = 0) throws -> Int {
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            throw KafkaError.unknownError
        }
        
        return Int(rd_kafka_poll(kafkaClientHandle, timeout))
        
    }
    
    public func produce(key: String? = nil, value: String, partition: Int32 = RD_KAFKA_PARTITION_UA) throws {
        
        guard let topicHandle = self.topicHandle else {
            throw KafkaError.unknownError
        }
        
        let selfRef = UnsafeMutableRawPointer(Unmanaged.passUnretained(self).toOpaque())
        
        let response = rd_kafka_produce(topicHandle,
                                    partition,
                                    RD_KAFKA_MSG_F_FREE,
                                    strdup(value),
                                    value.utf8.count,
                                    key ?? nil,
                                    key != nil ? key!.utf8.count : 0,
                                    selfRef)
        
        guard response == RD_KAFKA_RESP_ERR_NO_ERROR.rawValue else {
            
            let error = rd_kafka_last_error()
            throw KafkaError.coreError(KafkaCoreError(rdError: error))
            
        }
        
    }
    
    public func produce(key: Data? = nil, value: Data, partition: Int32 = RD_KAFKA_PARTITION_UA) throws {
        
        guard let topicHandle = self.topicHandle else {
            throw KafkaError.unknownError
        }
        
        let selfRef = UnsafeMutableRawPointer(Unmanaged.passUnretained(self).toOpaque())
        
        let valueBytes = [UInt8](value)
        let valueBuffer = malloc(valueBytes.count)
        let _ = valueBytes.withUnsafeBufferPointer {
            memcpy(valueBuffer, $0.baseAddress, valueBytes.count)
        }
        
        var keyBytes: [UInt8] = []
        var keyBuffer: UnsafeMutableRawPointer? = nil
        
        if let key = key {
            
            keyBytes = [UInt8](key)
            keyBuffer = malloc(keyBytes.count)
            
            let _ = keyBytes.withUnsafeBufferPointer {
                memcpy(keyBuffer, $0.baseAddress, keyBytes.count)
            }

        }
        
        let response = rd_kafka_produce(topicHandle,
                                        partition,
                                        RD_KAFKA_MSG_F_FREE,
                                        valueBuffer,
                                        valueBytes.count,
                                        keyBuffer,
                                        keyBytes.count,
                                        selfRef)
        
        guard response == RD_KAFKA_RESP_ERR_NO_ERROR.rawValue else {
            
            let error = rd_kafka_last_error()
            throw KafkaError.coreError(KafkaCoreError(rdError: error))
            
        }

        
    }
    
    // MARK: - Private Methods
    private func createKafkaTopic(withTopicConfig topicConfig: TopicConfig? = nil) throws {
        
        guard let t = rd_kafka_topic_new(kafkaClientHandle,
                                         topic,
                                         topicConfig == nil ? nil : topicConfig?.configHandle) else
        {
            
            let err = rd_kafka_last_error()
            throw KafkaError.coreError(KafkaCoreError(rdError: err))
            
        }
        
        self.topicHandle = t
        
    }
    
}
