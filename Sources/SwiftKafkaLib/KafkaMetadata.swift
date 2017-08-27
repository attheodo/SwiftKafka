//
//  KafkaMetadata.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 27/08/2017.
//
//

import ckafka

public struct BrokerMetadata {
    
    // MARK: - Public Properties
    
    /// The id of the broker
    public let id: Int
    
    /// The host of the broker
    public let host: String
    
    /// The port that the broker listens to
    public let port: Int
    
}

extension BrokerMetadata {
    
    static func brokers(fromRawBrokersData data: UnsafeMutablePointer<rd_kafka_metadata_broker>,
                        numOfBrokers: Int32) -> [BrokerMetadata]
    {
        
        var brokers: [BrokerMetadata] = []
        
        guard numOfBrokers > 0 else {
            return brokers
        }
        
        for i in 0...numOfBrokers - 1 {
            
            let b = data.advanced(by: Int(i)).pointee
            let broker = BrokerMetadata(id: Int(b.id), host: String(cString: b.host), port: Int(b.port))
            
            brokers.append(broker)
            
        }
        
        return brokers
        
    }

}

public struct PartitionMetadata {
    
    // MARK: - Public Properties
    
    /// The id of the parition
    public let id: Int
    
    /// A parition error as reported by the broker
    public let error: KafkaError?
    
    /// The leader of that broker
    public let leader: Int
    
    /// Replica brokers
    public let replicas: [Int]
    
    /// In-sync replica brokers
    public let inSyncReplicas: [Int]
    
}

extension PartitionMetadata {
    
    static func partitions(fromPartitionData data: UnsafeMutablePointer<rd_kafka_metadata_partition>,
                           numOfPartitions: Int32) -> [PartitionMetadata]
    {
        var partitions: [PartitionMetadata] = []
        
        guard numOfPartitions > 0 else {
            return partitions
        }
        
        for i in 0...numOfPartitions - 1 {
            
            let p = data.advanced(by: Int(i)).pointee
            
            var replicas: [Int] = []
            var inSyncReplicas: [Int] = []
            
            // get replicas
            for x in 0...p.replica_cnt - 1 {
                
                let replica = p.replicas.advanced(by: Int(x)).pointee
                replicas.append(Int(replica))
                
            }
            
            // get in-sync replicas
            for x in 0...p.isr_cnt - 1 {
                
                let replica = p.isrs.advanced(by: Int(x)).pointee
                inSyncReplicas.append(Int(replica))
                
            }
            
            let partition = PartitionMetadata(id: Int(p.id),
                                              error: KafkaError.coreError(KafkaCoreError(p.err)),
                                              leader: Int(p.leader),
                                              replicas: replicas,
                                              inSyncReplicas: inSyncReplicas)
            
            partitions.append(partition)
            
        }
        
        return partitions
        
    }

}

public struct TopicMetadata {
    
    // MARK: - Public Properties
    
    /// The name of the topic
    public let name: String
    
    /// Topic error as reported by the broker
    public let error: KafkaError?
    
    /// The partitions of this topic
    public let partitions: [PartitionMetadata]
    
}

extension TopicMetadata {
    
    static func topics(fromRawTopicsData data: UnsafeMutablePointer<rd_kafka_metadata_topic>,
                       numOfTopics: Int32) -> [TopicMetadata]
    {
        
        var topics: [TopicMetadata] = []
        
        guard numOfTopics > 0 else {
            return topics
        }
        
        for i in 0...numOfTopics - 1 {
            
            let t = data.advanced(by: Int(i)).pointee
            let topic = TopicMetadata(name: String(cString: t.topic),
                                      error: KafkaError.coreError(KafkaCoreError(t.err)),
                                      partitions: PartitionMetadata.partitions(fromPartitionData: t.partitions, numOfPartitions: t.partition_cnt))
            
            topics.append(topic)
            
        }
        
        return topics
        
    }

}

public struct Metadata {
    
    // MARK: - Public Properties
    
    /// The connected brokers
    public let brokers: [BrokerMetadata]
    
    /// The available topics
    public let topics: [TopicMetadata]
    
    /// Broker originating this metadata
    public let originatingBrokerId: Int
    
    /// Name of the originating broker
    public let originatingBrokerName: String
    
}

extension Metadata {
   
    static func metadata(fromRawMetadata data: rd_kafka_metadata) -> Metadata {
        
        let brokers = BrokerMetadata.brokers(fromRawBrokersData: data.brokers, numOfBrokers: data.broker_cnt)
        let topics = TopicMetadata.topics(fromRawTopicsData: data.topics, numOfTopics: data.topic_cnt)
        
        return Metadata(brokers: brokers,
                        topics: topics,
                        originatingBrokerId: Int(data.orig_broker_id),
                        originatingBrokerName: String(cString: data.orig_broker_name))
        
    }

}
