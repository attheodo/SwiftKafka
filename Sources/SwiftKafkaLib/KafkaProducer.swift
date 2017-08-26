//
//  KafkaProducer.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 26/08/2017.
//
//

import ckafka

class KafkaProducer: KafkaBase {
 
    // MARK: - Public Properties
    
    /// The number of messages waiting to be delivered
    /// to the broker
    public var numOfPendingMessages: Int32? {
        
        guard let h = handle else {
            return nil
        }
        
        return rd_kafka_outq_len(h)
        
    }
    
    // MARK: - Initialiser
    
    public init(globalConfiguration: GlobalConfig? = nil, topicConfiguration: TopicConfig? = nil) throws {
        
        let globalConfig = try (globalConfiguration ?? (try GlobalConfig()))
        let topicConfig = try (topicConfiguration ?? (try TopicConfig()))
        
        globalConfig.configureMessageDeliveryCallback()
        
        try super.init(withClientType: .producer, globalConfig: globalConfig, andTopicConfig: topicConfig)
        
    }
    
    deinit {
        let _ = try? flush(1000)
    }
    
    // MARK: - Public Methods
    
    /**
     Produce message to topic. This operation is asynchronous and you may want to pass in
     `deliveryClosure` that will be called from `poll()` when the message has been deliverd or permanently failed.
     - parameter topic: The name of the topic
     - parameter key: The message key
     - parameter value: The message value
     - parameter partition: The partition to produce too (defaults to default partitioner)
     - parameter deliveryClosure: A closure to execute if the message gets delivered or permanently failed.
    */
    public func produce(topic: String,
                        key: String? = nil,
                        value: String,
                        partition: Int32 = RD_KAFKA_PARTITION_UA,
                        deliveryClosure: MessageDeliveryClosure? = nil) throws
    {
        
        guard let h = handle else {
            throw KafkaError.unknownError
        }
        
        // config is being freed by `rd_kafka_topic_new` so we need to duplicate it and create a new instance
        let topicConfig = try TopicConfig(byDuplicatingConfig: topicConfiguration)
        
        guard let topicHandle = rd_kafka_topic_new(h, topic, topicConfig.handle) else {
            throw KafkaError.coreError(KafkaCoreError(rd_kafka_last_error()))
        }
        
        let msgContainer = MessageContainer(producerInstance: self, deliveryClosure: deliveryClosure)
        let msgContainerRef = UnsafeMutableRawPointer(Unmanaged.passRetained(msgContainer).toOpaque())
        
        let err = rd_kafka_produce(topicHandle,
                                   partition,
                                   RD_KAFKA_MSG_F_COPY,
                                   strdup(value),
                                   value.utf8.count,
                                   key ?? nil,
                                   key != nil ? key!.utf8.count : 0,
                                   msgContainerRef)
        
        if err != 0 {
            throw KafkaError.coreError(KafkaCoreError(rd_kafka_last_error()))
        }
        
        rd_kafka_topic_destroy(topicHandle)
        
    }
    
    /**
     Wait for all messages in the producer queue to be delivered. 
     - **NOTE:** This method will trigger callbacks.
     - parameter timeout: The time to wait in milliseconds
     - returns: The number of messages still in the queue
     */
    @discardableResult
    public func flush(_ timeout: Int32 = 100) throws -> Int32? {
        
        guard let h = handle else {
            return nil
        }
        
        let error = rd_kafka_flush(h, timeout)
        
        if error != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.coreError(KafkaCoreError(error))
        }
        
        return numOfPendingMessages
        
    }
    
    /**
     Polls the broker for events. Events will cause application provided callbacks to be called.
     - parameter timeout: Specifies the maximum amount of time (in ms) that the call will block waiting for events.
        - For non-blocking calls, set `timeout` to `0`
        - To wait indefinately, set `timeout` to `-1`
     - returns: The number of events served
    */
    @discardableResult
    public func poll(timeout: Int32 = 0) throws -> Int {
        
        guard let h = handle else {
            throw KafkaError.unknownError
        }
        
        return Int(rd_kafka_poll(h, timeout))
        
    }
    
}
