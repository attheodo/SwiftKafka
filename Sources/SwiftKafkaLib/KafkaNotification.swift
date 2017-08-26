internal protocol KafkaNotification {
    
    static func fromDict(dict: [AnyHashable: Any]) -> Self?
    func toDict() -> [String: Self]
    
}

extension KafkaNotification {
    
    static func fromDict(dict: [AnyHashable: Any]) -> Self? {
        
        guard let payload = dict["payload"] as? Self else {
            return nil
        }
        
        return payload
        
    }
    
    func toDict() -> [String: Self] {
        return ["payload": self]
    }

}

internal enum KafkaNotificationType {
    
    enum Producer {
        case messageReceived
    }
    
    enum Consumer {
    
        case partitionsAssigned
        case partitionsRevoked
        case offsetsCommitted
        
    }
    
}

internal struct KafkaConsumerNotification: KafkaNotification {
    
    // MARK: - Public Properties
    
    let notificationType: KafkaNotificationType.Consumer
    let partitions: [KafkaTopicPartition]
    let error: KafkaError?

    // MARK: - Initialiser
    
    init(notificationType: KafkaNotificationType.Consumer,
         partitions: [KafkaTopicPartition],
         error: KafkaError? = nil)
    {
        
        self.notificationType = notificationType
        self.partitions = partitions
        self.error = error
        
    }
    
}

internal struct KafkaProducerNotification: KafkaNotification {
    
    // MARK: - Public Properties
    
    let notificationType: KafkaNotificationType.Producer
    let message: KafkaMessage?
    let error: KafkaError?
    
    // MARK: - Initialiser
    
    init(notificationType: KafkaNotificationType.Producer,
         message: KafkaMessage?,
         error: KafkaError? = nil)
    {
        
        self.notificationType = notificationType
        self.message = message
        self.error = error
        
    }

}
