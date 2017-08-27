//
//  TopicPartition.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 27/08/2017.
//
//

import ckafka

public struct TopicPartition {
    
    // MARK: - Public Properties
    let topic: String
    let partition: Int32
    let offset: OffsetPosition
    let error: KafkaError?
    
}

extension TopicPartition {
    
    // MARK: - Static
    
    /** 
      Returns an `OffsetPosition` enum from the passed value
      - parameter value: The value for which to return an `OffsetPosition`
    */
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
    
    /**
     Converts `rd_kafka_topic_partition` to `TopicPartition`
    */
    public static func topicPartition(fromRawPartition partition: UnsafeMutablePointer<rd_kafka_topic_partition_t>) -> TopicPartition
    {
        
        let topicPartition = partition.pointee
        
        var error: KafkaError? = nil
        
        let offset = TopicPartition.offset(fromRawOffsetValue: topicPartition.offset)
        
        // set the error if any
        if topicPartition.err != RD_KAFKA_RESP_ERR_NO_ERROR {
            error = KafkaError.coreError(KafkaCoreError(topicPartition.err))
        }
        
        
        return TopicPartition(topic: String(cString: topicPartition.topic),
                              partition: topicPartition.partition,
                              offset: offset,
                              error: error)
        
    }
    
    /**
     Converts an array of `TopicPartition` to `rd_kafka_topic_partition_list_t`
    */
    public static func rawPartitionList(fromPartitions partitions: [TopicPartition]) -> UnsafeMutablePointer<rd_kafka_topic_partition_list_t>?
    {
        
        let partitionList = rd_kafka_topic_partition_list_new(Int32(partitions.count))
        
        partitions.forEach { partition in
            
            let p = rd_kafka_topic_partition_list_add(partitionList,
                                                      partition.topic.cString(using: .utf8),
                                                      partition.partition)
            
            p?.pointee.offset = partition.offset.value()
            
        }
        
        return partitionList
        
    }
    
    /**
     Converts a `rd_kafka_topic_partition_list_t` to an array of `TopicPartition`
    */
    public static func partitions(fromPartitionList partitionList: UnsafeMutablePointer<rd_kafka_topic_partition_list_t>) -> [TopicPartition] {
        
        var partitions: [TopicPartition] = []
        let partitionList = partitionList.pointee
        
        guard partitionList.cnt > 0 else {
            return []
        }
        
        for i in 0...partitionList.cnt - 1 {
            
            let cPartition = partitionList.elems.advanced(by: Int(i)).pointee
            
            var error: KafkaError? = nil
            
            if cPartition.err != RD_KAFKA_RESP_ERR_NO_ERROR {
                error = KafkaError.coreError(KafkaCoreError(cPartition.err))
            }
            
            let partition = TopicPartition(topic: String(cString: cPartition.topic),
                                           partition: cPartition.partition,
                                           offset: TopicPartition.offset(fromRawOffsetValue: cPartition.offset),
                                           error: error)
            
            partitions.append(partition)
            
        }
        
        return partitions
        
    }
    
}
