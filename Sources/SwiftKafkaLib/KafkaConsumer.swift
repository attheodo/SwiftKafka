import Foundation
import ckafka

public enum OffsetPosition {
    
    /// Start consuming from the beginning of 
    /// Kafka partition queue (oldest msg)
    case beginning
    
    /// Start consuming from the end of 
    ///Kafka partition queue (next msg)
    case end
    
    /// Start consuming from the offset retrieved 
    /// from the offset store
    case stored
    
    case invalid
    
    /// Start consuming `offset` messages from 
    /// the topic's current offset
    /// - If current offset is 1000 and `offset` is `10`, it will start 
    /// consuming from `1000 - 10 = 990`
    case tail(Int64)
    
    /// Start consuming from the specified offset
    case specific(Int64)
    
    func value() -> Int64 {
        
        switch self {
        
        case .beginning:
            return Int64(RD_KAFKA_OFFSET_BEGINNING)
        
            
        case .invalid:
            return Int64(RD_KAFKA_OFFSET_INVALID)
            
        case .end:
            return Int64(RD_KAFKA_OFFSET_END)
        
        case .stored:
            return Int64(RD_KAFKA_OFFSET_STORED)
        
        case .tail(let offset):
            return (Int64(RD_KAFKA_OFFSET_TAIL_BASE) - offset)
            
        case .specific(let offset):
            return offset
        }
        
    }
    
}

class KafkaConsumer: Kafka {
    
    // MARK: - Public Properties
    
    /// Returns a tuple containing the current partition assignments and 
    /// an optional error if any
    public var assignment: (partitions: [KafkaTopicPartition]?, error: KafkaError?) {
       
        do {
        
            let partitions = try getAssignment()
            return (partitions.count > 0 ? partitions : nil, nil)
        
        } catch (let error) {
            return (nil, error as? KafkaError)
        }
        
    }
    
    /// Internal variable for keeping track whether SwiftKafka needs
    /// to manually call `assign()` in order to sync state.
    public private(set) var rebalanceAssigned: Int = 0
    
    public var onPartitionAssign: (([KafkaTopicPartition]) -> Void)?
    public var onPartitionRevoke: (([KafkaTopicPartition]) -> Void)?
    public var onOffsetsCommit: ((_ error: KafkaError?, _ partitions: [KafkaTopicPartition]) -> Void)?
    
    // MARK: - Private Properties
    private var kafkaConfig: KafkaConfig?
    
    // MARK: - Initialiser
    
    public init(kafkaConfig: KafkaConfig? = nil) throws {
        
        self.kafkaConfig = try (kafkaConfig ?? (try KafkaConfig()))
        
        kafkaConfig?.configureOffsetCommitCallback()
        kafkaConfig?.configureRebalanceCallback()
        
        try super.init(withClientType: .consumer, andConfig: kafkaConfig)
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            throw KafkaError.unknownError
        }
        
        let error = rd_kafka_poll_set_consumer(kafkaClientHandle)
        
        if error != RD_KAFKA_RESP_ERR_NO_ERROR {
            throw KafkaError.coreError(KafkaCoreError(rdError: error))
        }
        
        registerNotifications()
        
    }
    
    deinit {

        guard let kafkaClientHandle = self.kafkaClientHandle else {
            return
        }
        
        // wait for the queue to be flushed making sure any
        // offset commits make it to the server
        while(rd_kafka_outq_len(kafkaClientHandle) > 0) {
            rd_kafka_poll(kafkaClientHandle, 100)
        }
        
        rd_kafka_destroy(kafkaClientHandle)

    }
    
    // MARK: - Public Methods
    
    /**
     Sets consumer partition assignment to the provided list of `KafkaTopicPartitions` and starts consuming.
     - parameter partitions: The list of topic+partitions (optionally with initial offsets) to start consuming.
    */
    public func assign(topicPartitions partitions: [KafkaTopicPartition]) throws {
        
        guard let kafkaClientHandle = self.kafkaClientHandle,
              let cPartitions = KafkaTopicPartition.rawCPartitionList(fromPartitions: partitions) else
        {
            throw KafkaError.unknownError
        }
        
        defer {
            rd_kafka_topic_partition_list_destroy(cPartitions)
        }
        
        let error = rd_kafka_assign(kafkaClientHandle, cPartitions)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(rdError: error))
        }
        
        rebalanceAssigned += 1
        
    }
 
    /**
     Set subscription to supplied list of topics. This replaces any previous subscription.
     - Regexp pattern subscriptions are supported by prefixing the topic string with "^" 
        - e.g: `consumer.subscribe(["^my_topic.*", "^another[0-9]-?[a-z]+$", "not_a_regex"])`
    */
    public func subscribe(toTopics topics: [String]) throws {
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            throw KafkaError.unknownError
        }
        
        var topicsList = rd_kafka_topic_partition_list_new(Int32(topics.count))
        
        defer {
           rd_kafka_topic_partition_list_destroy(topicsList)
        }
        
        topics.forEach { topic in
            rd_kafka_topic_partition_list_add(topicsList, topic, RD_KAFKA_PARTITION_UA)
        }
        
        let error = rd_kafka_subscribe(kafkaClientHandle, topicsList)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(rdError: error))
        }

        
    }
    
    /**
     Retrieve committed offsets for a list of partitions.
     - parameter topicPartitions: An array of topic partitions for which to receive the committed offsets
     - returns: An array of topic partitions with `offset` and possibly `error` properly set from the broker.
    */
    public func committedOffsets(forTopicPartitions topicPartitions: [KafkaTopicPartition],
                                 timeout: Int32 = 100) throws -> [KafkaTopicPartition]
    {
        
        guard let kafkaClientHandle = self.kafkaClientHandle,
              let cPartitions = KafkaTopicPartition.rawCPartitionList(fromPartitions: topicPartitions) else
        {
            throw KafkaError.unknownError
        }
        
        defer {
            rd_kafka_topic_partition_list_destroy(cPartitions)
        }
        
        let error = rd_kafka_committed(kafkaClientHandle, cPartitions, timeout)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(rdError: error))
        }
        
        return KafkaTopicPartition.partitions(fromCPartitionsList: cPartitions)

    }
    
    public func close() {
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            return
        }
        
        rd_kafka_consumer_close(kafkaClientHandle)
        
    }
    
    /**
     Commit the current partition assignment's offsets
     - parameter async: Whether the operation should be execute asynchronously.
     If `false` it will return immediately.
     - **NOTE**: The consumer relies on the use of this method if `enable.auto.commit=false`
    */
    public func commit(async: Bool = true) throws {
        let items: [Int]? = nil
        try genericCommit(items: items, async: async)
    }
    
    /**
     Commit the offsets of the passed topic + partitions list
     - parameter async: Whether the operation should execute asynchronously.
     If `false` it will return immediately.
     - **NOTE**: The consumer relies on the use of this method if `enable.auto.commit=false`
    */
    public func commit(topicPartitions partitions: [KafkaTopicPartition], async: Bool = true) throws {
        try genericCommit(items: partitions, async: async)
    }
    
    /**
     Commit the the message's offset + 1
     - parameter async: Whether the operation should execute asynchronously.
     If `false it will return immediately.
     - **NOTE**: The consumer relies on the use of this method if `enable.auto.commit=false`
    */
    public func commit(messages: [KafkaMessage], async: Bool = true) throws {
        try genericCommit(items: messages, async: async)
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
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            return nil
        }
        
        let m = rd_kafka_consumer_poll(kafkaClientHandle, timeout)
        
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
    
    /// Removes the current partition assignment and stops consuming
    public func unassign() throws {
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            throw KafkaError.unknownError
        }
        
        let error = rd_kafka_assign(kafkaClientHandle, nil)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(rdError: error))
        }
        
        rebalanceAssigned += 1
        
    }
    
    /// Remove the current subscription
    public func unsubscribe() throws {
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            throw KafkaError.unknownError
        }
        
        let error = rd_kafka_unsubscribe(kafkaClientHandle)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(rdError: error))
        }
        
    }
    
    /**
     Retrieve current positions (offsets) for topics+partitions.
     
     The `offset` field of each requested partition will be set to the offset of the last consumed message + 1, 
     or `.invalid` in case there was no previous message.

     - parameter topicPartitions: An array of topic partitions to return the current offsets for.
     - returns: An array of topic partitions with `offets` and possibly `error` set accordingly from the broker
     
    */
    public func position(forTopicPartitions topicPartitions: [KafkaTopicPartition]) throws -> [KafkaTopicPartition] {
        
        guard let kafkaClientHandle = self.kafkaClientHandle,
              let cPartitions = KafkaTopicPartition.rawCPartitionList(fromPartitions: topicPartitions) else
        {
            throw KafkaError.unknownError
        }
        
        defer {
            rd_kafka_topic_partition_list_destroy(cPartitions)
        }
        
        let error = rd_kafka_position(kafkaClientHandle, cPartitions)

        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(rdError: error))
        }
        
        return KafkaTopicPartition.partitions(fromCPartitionsList: cPartitions)

    }
    
    // MARK: - Private Methods
    
    private func genericCommit<T>(items: [T]? = nil, async: Bool = true) throws {
        
        guard let kafkaClientHandle = self.kafkaClientHandle else {
            throw KafkaError.unknownError
        }
        
        var cPartitions: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>? = nil
        
        defer {
            
            if let p = cPartitions {
                rd_kafka_topic_partition_list_destroy(p)
            }
            
        }
        
        if items is [KafkaTopicPartition] {
            
            guard let p = KafkaTopicPartition.rawCPartitionList(fromPartitions: items as! [KafkaTopicPartition]) else {
                throw KafkaError.unknownError
            }
            
            cPartitions = p
            
        } else if items is [KafkaMessage] {
            
            cPartitions = rd_kafka_topic_partition_list_new(Int32(items!.count))
            
            for message in items as! [KafkaMessage] {
                
                let partition = rd_kafka_topic_partition_list_add(cPartitions,
                                                                  message.topic?.cString(using: .utf8),
                                                                  message.partition)
                partition?.pointee.offset = message.offset + 1
                
            }
            
        }
        
        let error = rd_kafka_commit(kafkaClientHandle,
                                    cPartitions,
                                    async ? 1 : 0)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(rdError: error))
        }
        
    }
    
    private func getAssignment() throws -> [KafkaTopicPartition] {

        guard let kafkaClientHandle = self.kafkaClientHandle else {
            throw KafkaError.unknownError
        }
        
        let partitions = UnsafeMutablePointer<UnsafeMutablePointer<rd_kafka_topic_partition_list_t>?>.allocate(capacity: 1)
        
        let error = rd_kafka_assignment(kafkaClientHandle, partitions)
        
        guard error == RD_KAFKA_RESP_ERR_NO_ERROR else {
            throw KafkaError.coreError(KafkaCoreError(rdError: error))
        }
        
        if let p = partitions.pointee {
            return KafkaTopicPartition.partitions(fromCPartitionsList: p)
        } else {
            throw KafkaError.unknownError
        }
        
    }
    
    // MARK: - Notifications
    private func registerNotifications() {
        
        guard let consumerName = self.name else {
            return
        }
        
        let nc = NotificationCenter.default
        
        nc.addObserver(self,
                       selector: #selector(notificationHandler(notification:)),
                       name: Notification.Name(consumerName),
                       object: nil)
    
    }
    
    // MARK: Notification Selectors
    @objc private func notificationHandler(notification: Notification) {
        
        guard let userInfo = notification.userInfo,
              let kafkaNotification = KafkaConsumerNotification.fromDict(dict: userInfo) else
        {
            return
        }
        
        rebalanceAssigned = 0
        
        switch kafkaNotification.notificationType {
        
        case .partitionsAssigned:
         
            if let callback = self.onPartitionAssign {
                callback(kafkaNotification.partitions)
            } else {
                rd_kafka_yield(kafkaClientHandle)
            }
            
            if rebalanceAssigned == 0 {
                
                let parts = KafkaTopicPartition.rawCPartitionList(fromPartitions: kafkaNotification.partitions)
                rd_kafka_assign(kafkaClientHandle, parts)
            
            }
            
        case .partitionsRevoked:
            
            if let callback = self.onPartitionRevoke {
                callback(kafkaNotification.partitions)
            } else {
                rd_kafka_yield(kafkaClientHandle)
            }
            
            if rebalanceAssigned == 0 {
                rd_kafka_assign(kafkaClientHandle, nil)
            }
        
        case .offsetsCommitted:
            
            if let callback = self.onOffsetsCommit {
                callback(kafkaNotification.error, kafkaNotification.partitions)
            } else {
                rd_kafka_yield(kafkaClientHandle)
            }
        
        }
        
    }
    
}
