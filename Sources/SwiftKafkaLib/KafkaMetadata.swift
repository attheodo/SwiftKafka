import ckafka

public struct Broker {
    
    // MARK: - Static
    static func brokers(fromRawBrokersData data: UnsafeMutablePointer<rd_kafka_metadata_broker>,
                        numOfBrokers: Int32) -> [Broker]
    {
        
        var brokers: [Broker] = []
        
        guard numOfBrokers > 0 else {
            return brokers
        }
        
        for i in 0...numOfBrokers - 1 {
            
            let b = data.advanced(by: Int(i)).pointee
            let broker = Broker(id: Int(b.id), host: String(cString: b.host), port: Int(b.port))
            
            brokers.append(broker)
            
        }
        
        return brokers
        
    }
    
    // MARK: - Public Properties
    
    /// The id of the broker
    public let id: Int
    
    /// The host of the broker
    public let host: String
    
    /// The port that the broker listens to
    public let port: Int
    
}

public struct Partition {
    
    // MARK: - Static
    static func partitions(fromPartitionData data: UnsafeMutablePointer<rd_kafka_metadata_partition>,
                        numOfPartitions: Int32) -> [Partition]
    {
        var partitions: [Partition] = []
        
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
            
            let partition = Partition(id: Int(p.id),
                                      error: KafkaError.coreError(KafkaCoreError(rdError: p.err)),
                                      leader: Int(p.leader),
                                      replicas: replicas,
                                      inSyncReplicas: inSyncReplicas)
            
            partitions.append(partition)
            
        }
        
        return partitions
        
    }
    
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

public struct Topic {
    
    // MARK: - Static
    static func topics(fromRawTopicsData data: UnsafeMutablePointer<rd_kafka_metadata_topic>,
                       numOfTopics: Int32) -> [Topic]
    {
        
        var topics: [Topic] = []
        
        guard numOfTopics > 0 else {
            return topics
        }
        
        for i in 0...numOfTopics - 1 {
            
            let t = data.advanced(by: Int(i)).pointee
            let topic = Topic(name: String(cString: t.topic),
                              error: KafkaError.coreError(KafkaCoreError(rdError: t.err)),
                              partitions: Partition.partitions(fromPartitionData: t.partitions,
                                                              numOfPartitions: t.partition_cnt))
            
            topics.append(topic)
            
        }
        
        return topics

    }
    
    // MARK: - Public Properties
    
    /// The name of the topic
    public let name: String
    
    /// Topic error as reported by the broker
    public let error: KafkaError?
    
    /// The partitions of this topic
    public let partitions: [Partition]
    
}

public struct Metadata {
    
    // MARK: - Static
    static func metadata(fromRawMetadata data: rd_kafka_metadata) -> Metadata {
        
        let brokers = Broker.brokers(fromRawBrokersData: data.brokers, numOfBrokers: data.broker_cnt)
        let topics = Topic.topics(fromRawTopicsData: data.topics, numOfTopics: data.topic_cnt)
        
        return Metadata(brokers: brokers,
                        topics: topics,
                        originatingBrokerId: Int(data.orig_broker_id),
                        originatingBrokerName: String(cString: data.orig_broker_name))
        
    }
    
    // MARK: - Public Properties
    
    /// The connected brokers
    public let brokers: [Broker]
    
    /// The available topics
    public let topics: [Topic]
    
    /// Broker originating this metadata
    public let originatingBrokerId: Int
    
    /// Name of the originating broker
    public let originatingBrokerName: String
    
}
