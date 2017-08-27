//
//  KafkaConsumer.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 27/08/2017.
//
//

import Foundation

import ckafka

class KafkaConsumer: KafkaBase {
    
    // MARK: - Public Properties
    /// Returns a tuple containing the current partition assignments and
    /// an optional error if any
    public var assignment: (partitions: [TopicPartition]?, error: KafkaError?) {
        
        do {
            
            let partitions = try getAssignment()
            return (partitions.count > 0 ? partitions : nil, nil)
            
        } catch (let error) {
            return (nil, error as? KafkaError)
        }
        
    }
    
    /// Internal variable for keeping track whether SwiftKafka needs
    /// to manually call `assign()` in order to sync state.
    internal private(set) var rebalanceAssigned: Int = 0
    
    /// A closure to execute when offsets are committed
    public var onOffsetsCommit: ((_ partitions: [TopicPartition], _ error: KafkaError?) -> Void)?
    
    /// A closure to execute when partitions are assigned to the consumer
    public var onPartitionAssign: (([TopicPartition]) -> Void)?
    
    /// A closure to execute when partitions are revoked from the consumer
    public var onPartitionRevoke: (([TopicPartition]) -> Void)?
    
    // MARK: - Initialiser
    
    public init(globalConfiguration: GlobalConfig? = nil, topicConfiguration: TopicConfig? = nil) throws {
        
        let globalConfig = try (globalConfiguration ?? (try GlobalConfig()))
        let topicConfig = try (topicConfiguration ?? (try TopicConfig()))
        
        
        globalConfig.configureOffsetCommitCallback()
        globalConfig.configureRebalanceCallback()
        
        try super.init(withClientType: .consumer, globalConfig: globalConfig, andTopicConfig: topicConfig)
        
        guard let h = handle else {
            throw KafkaError.unknownError
        }
        
        let error = rd_kafka_poll_set_consumer(h)
        
        if error != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.coreError(KafkaCoreError(error))
        }
        
        self.globalConfiguration?.setOpaquePointer(forKafkaClient: self)
        
    }
    
    deinit {
        
        guard let h = handle else {
            return
        }
        
        // wait for the queue to be flushed making sure any
        // offset commits make it to the server
        while(rd_kafka_outq_len(h) > 0) {
            rd_kafka_poll(h, 100)
        }
        
        rd_kafka_destroy(h)
        
    }
    
    // MARK: - Public Methods
    
    /**
     Sets consumer partition assignment to the provided list of `TopicPartitions` and starts consuming.
     - parameter partitions: The list of topic+partitions (optionally with initial offsets) to start consuming.
     */
    public func assign(topicPartitions partitions: [TopicPartition]) throws {
        
        guard let h = handle,
              let partitions = TopicPartition.rawPartitionList(fromPartitions: partitions) else
        {
            throw KafkaError.unknownError
        }
        
        defer {
            rd_kafka_topic_partition_list_destroy(partitions)
        }
        
        let error = rd_kafka_assign(h, partitions)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(error))
        }
        
        rebalanceAssigned += 1
        
    }
    
    /**
     Set subscription to supplied list of topics. This replaces any previous subscription.
     - Regexp pattern subscriptions are supported by prefixing the topic string with "^"
     - e.g: `consumer.subscribe(["^my_topic.*", "^another[0-9]-?[a-z]+$", "not_a_regex"])`
     */
    public func subscribe(toTopics topics: [String]) throws {
        
        guard let h = handle else {
            throw KafkaError.unknownError
        }
        
        var topicsList = rd_kafka_topic_partition_list_new(Int32(topics.count))
        
        defer {
            rd_kafka_topic_partition_list_destroy(topicsList)
        }
        
        topics.forEach { topic in
            rd_kafka_topic_partition_list_add(topicsList, topic, RD_KAFKA_PARTITION_UA)
        }
        
        let error = rd_kafka_subscribe(h, topicsList)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(error))
        }
        
    }
    
    /**
     Retrieve committed offsets for a list of partitions.
     - parameter topicPartitions: An array of topic partitions for which to receive the committed offsets
     - returns: An array of topic partitions with `offset` and possibly `error` properly set from the broker.
     */
    public func committedOffsets(forTopicPartitions topicPartitions: [TopicPartition],
                                 timeout: Int32 = 100) throws -> [TopicPartition]
    {
        
        guard let h = handle,
            let partitions = TopicPartition.rawPartitionList(fromPartitions: topicPartitions) else
        {
            throw KafkaError.unknownError
        }
        
        defer {
            rd_kafka_topic_partition_list_destroy(partitions)
        }
        
        let error = rd_kafka_committed(h, partitions, timeout)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(error))
        }
        
        return TopicPartition.partitions(fromPartitionList: partitions)
        
    }
    
    /**
     Close down and terminate the Kafka consumer.
     
     - **NOTES:** This call will block until the consumer has:
        - Revoked its assignment. 
        - Called the rebalance_cb if it is configured 
        - Committed offsets to broker 
        - Left the consumer group. 
     
     The maximum blocking time is roughly limited to `session.timeout.ms`.
    */
    public func close() {
        
        guard let h = handle else {
            return
        }
        
        rd_kafka_consumer_close(h)
        
    }
    
    /**
     Commit the current partition assignment's offsets
     - parameter async: Whether the operation should be execute asynchronously. If `false` it will return immediately.
     - **NOTE**: The consumer relies on the use of this method if `enable.auto.commit=false`
     */
    public func commit(async: Bool = true) throws {
        let items: [Int]? = nil
        try genericCommit(items: items, async: async)
    }
    
    /**
     Commit the offsets of the passed topic + partitions list
     - parameter async: Whether the operation should execute asynchronously. If `false` it will return immediately.
     - **NOTE**: The consumer relies on the use of this method if `enable.auto.commit=false`
     */
    public func commit(topicPartitions partitions: [TopicPartition], async: Bool = true) throws {
        try genericCommit(items: partitions, async: async)
    }
    
    /**
     Commit the the message's offset + 1
     - parameter async: Whether the operation should execute asynchronously. If `false it will return immediately.
     - **NOTE**: The consumer relies on the use of this method if `enable.auto.commit=false`
     */
    public func commit(messages: [KafkaMessage], async: Bool = true) throws {
        try genericCommit(items: messages, async: async)
    }
    
    /**
     Retrieves the low and high offsets for a `TopicPartition`
     - parameter topicPartition: The `TopicPartition` to return offsets for
     - parameter timeout: The request timeout (only if `cached=false`)
     - parameter cached: A boolean indicating whether to use cached information
        - Regarding cached information: The low offset is updated periodically (if `statistics.interval.ms` is set)
          while the high offset is updated on each message fetched from the broker for this partition.
     - returns: A tuple with the `low` and `high` offsets
    */
    public func getWatermarkOffsets(forTopicPartition topicPartition: TopicPartition,
                                    timeout: Int32 = 1000,
                                    cached: Bool = false) throws -> (low: Int64, high: Int64)
    {
        
        guard let h = handle else {
            throw KafkaError.unknownError
        }
        
        let low = UnsafeMutablePointer<Int64>.allocate(capacity: 1)
        let high = UnsafeMutablePointer<Int64>.allocate(capacity: 1)
        
        defer {
            
            low.deallocate(capacity: 1)
            high.deallocate(capacity: 1)
            
        }
        
        var err: rd_kafka_resp_err_t
        
        if cached {
            err = rd_kafka_get_watermark_offsets(h, topicPartition.topic, topicPartition.partition, low, high)
        } else {
            err = rd_kafka_query_watermark_offsets(h, topicPartition.topic, topicPartition.partition, low, high, timeout)
        }
        
        guard err == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(err))
        }
        
        return (low: low.pointee, high: high.pointee)
        
    }
    
    /**
     Poll the consumer for messages or events. Triggers the callbacks.
     
     **Notes:** An application should make sure to call `poll()` at regular intervals,
     even if no messages are expected, to serve any queued callbacks waiting to be called.
     This is especially important when a rebalance_cb has been registered as it needs to
     be called and handled properly to synchronize internal consumer state.
     
     - parameter timeout: Maximum time to block waiting for message, event of callback
     - returns: An optional `KafkaMessage` object which is a proper message if it's `error`
     property is `nil` or an event or error for any other value
     
     */
    public func poll(timeout: Int32 = -1) -> KafkaMessage? {
        
        guard let h = handle else {
            return nil
        }
        
        let m = rd_kafka_consumer_poll(h, timeout)
        
        defer {
            
            if m != nil {
                rd_kafka_message_destroy(m)
            }
            
        }
        
        guard let msg = m?.pointee else {
            return nil
        }
        
        return KafkaMessage.message(fromRawMessage: msg)
        
    }
    
    /**
     Retrieve current positions (offsets) for topics+partitions.
     
     The `offset` field of each requested partition will be set to the offset of the last consumed message + 1,
     or `.invalid` in case there was no previous message.
     - parameter topicPartitions: An array of topic partitions to return the current offsets for.
     - returns: An array of topic partitions with `offets` and possibly `error` set accordingly from the broker
     
     */
    public func position(forTopicPartitions topicPartitions: [TopicPartition]) throws -> [TopicPartition] {
        
        guard let h = handle,
            let partitions = TopicPartition.rawPartitionList(fromPartitions: topicPartitions) else
        {
            throw KafkaError.unknownError
        }
        
        defer {
            rd_kafka_topic_partition_list_destroy(partitions)
        }
        
        let error = rd_kafka_position(h, partitions)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(error))
        }
        
        return TopicPartition.partitions(fromPartitionList: partitions)
        
    }
    
    internal func resetRebalanceAssignment() {
        rebalanceAssigned = 0
    }
    
    /// Removes the current partition assignment and stops consuming
    public func unassign() throws {
        
        guard let h = handle else {
            throw KafkaError.unknownError
        }
        
        let error = rd_kafka_assign(h, nil)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(error))
        }
        
        rebalanceAssigned += 1
        
    }
    
    /// Remove the current subscription
    public func unsubscribe() throws {
        
        guard let h = handle else {
            throw KafkaError.unknownError
        }
        
        let error = rd_kafka_unsubscribe(h)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(error))
        }
        
    }
    
    // MARK: - Private Methods
    
    private func getAssignment() throws -> [TopicPartition] {
        
        guard let h = handle else {
            throw KafkaError.unknownError
        }
        
        let partitions = UnsafeMutablePointer<UnsafeMutablePointer<rd_kafka_topic_partition_list_t>?>.allocate(capacity: 1)
        
        let error = rd_kafka_assignment(h, partitions)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(error))
        }
        
        if let p = partitions.pointee {
            return TopicPartition.partitions(fromPartitionList: p)
        } else {
            throw KafkaError.unknownError
        }
        
    }
    
    private func genericCommit<T>(items: [T]? = nil, async: Bool = true) throws {
        
        guard let h = handle else {
            throw KafkaError.unknownError
        }
        
        var partitions: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>? = nil
        
        defer {
            
            if let p = partitions {
                rd_kafka_topic_partition_list_destroy(p)
            }
            
        }
        
        if items is [TopicPartition] {
            
            guard let p = TopicPartition.rawPartitionList(fromPartitions: items as! [TopicPartition]) else {
                throw KafkaError.unknownError
            }
            
            partitions = p
            
        } else if items is [KafkaMessage] {
            
            partitions = rd_kafka_topic_partition_list_new(Int32(items!.count))
            
            for message in items as! [KafkaMessage] {
                
                let partition = rd_kafka_topic_partition_list_add(partitions,
                                                                  message.topic?.cString(using: .utf8),
                                                                  message.partition)
                partition?.pointee.offset = message.offset + 1
                
            }
            
        }
        
        let error = rd_kafka_commit(h, partitions, async ? 1 : 0)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(error))
        }
        
    }

}
