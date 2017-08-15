import ckafka

public struct KafkaTopicPartition {
    
    // MARK: - Static
    
    public static func offset(fromRawOffsetValue value: Int64) -> OffsetPosition {
        
        switch Int32(value) {
            
        case RD_KAFKA_OFFSET_END:
            return .end
            
        case RD_KAFKA_OFFSET_STORED:
            return .stored
            
        case RD_KAFKA_OFFSET_INVALID:
            return .invalid
            
        case RD_KAFKA_OFFSET_BEGINNING:
            return .beginning
            
        default:
            return .specific(Int64(value))
            
        }

    }
    
    public static func topicPartition(fromRawData data: UnsafeMutablePointer<rd_kafka_topic_partition_t>) -> KafkaTopicPartition {
        
        let topicPartition = data.pointee
        
        var error: KafkaError? = nil
        
        let offset = KafkaTopicPartition.offset(fromRawOffsetValue: topicPartition.offset)
        
        // set the error if any
        if topicPartition.err != RD_KAFKA_RESP_ERR_NO_ERROR {
            error = KafkaError.coreError(KafkaCoreError(rdError: topicPartition.err))
        }

        
        return KafkaTopicPartition(topic: String(cString: topicPartition.topic),
                                   partition: topicPartition.partition,
                                   offset: offset,
                                   error: error)
        
    }
    
    public static func rawCPartitionList(fromPartitions partitions: [KafkaTopicPartition]) -> UnsafeMutablePointer<rd_kafka_topic_partition_list_t>? {
        
        let cPartitions = rd_kafka_topic_partition_list_new(Int32(partitions.count))
        
        partitions.forEach { partition in
            rd_kafka_topic_partition_list_add(cPartitions,
                                              partition.topic.cString(using: .utf8),
                                              partition.partition)
        }
        
        return cPartitions
        
    }
    
    public static func partitions(fromCPartitionsList cPartitionList: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>) -> [KafkaTopicPartition] {
        
        var partitions: [KafkaTopicPartition] = []
        let cPartitionList = cPartitionList.pointee
        
        guard cPartitionList.cnt > 0 else {
            return []
        }
        
        for i in 0...cPartitionList.cnt - 1 {
        
            let cPartition = cPartitionList.elems.advanced(by: Int(i)).pointee
            
            var error: KafkaError? = nil

            if cPartition.err != RD_KAFKA_RESP_ERR_NO_ERROR {
                error = KafkaError.coreError(KafkaCoreError(rdError: cPartition.err))
            }
            
            let partition = KafkaTopicPartition(topic: String(cString: cPartition.topic),
                                                partition: cPartition.partition,
                                                offset: KafkaTopicPartition.offset(fromRawOffsetValue: cPartition.offset),
                                                error: error)
            
            partitions.append(partition)
        
        }
        
        return partitions
        
    }
    
    // MARK: - Public Properties
    let topic: String
    let partition: Int32
    let offset: OffsetPosition
    let error: KafkaError?
    
}
