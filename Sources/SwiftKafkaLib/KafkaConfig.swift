import Foundation
import ckafka

public struct KafkaConfig {
    
    // MARK: - Public Properties
    public var properties: [String: String]? {
        return listConfigurationProperties()
    }
    
    // MARK: - Public Properties
    public private(set) var configHandle: OpaquePointer
    
    // MARK: - Initialiser
    init(byDuplicatingConfig config: KafkaConfig? = nil) throws {
        
        if let config = config {
            
            guard let cfg = rd_kafka_conf_dup(config.configHandle) else {
                throw KafkaError.configDuplicationError
            }
            
            configHandle = cfg
            
        } else {
            
            guard let cfg = rd_kafka_conf_new() else {
                throw KafkaError.configCreationError
            }
            
            configHandle = cfg
            
        }
        
    }
    
    // MARK: - Public Methods
    
    /**
     Returns the value of a configuration variable
     - parameter variable: The name of the variable for which to get the value
     - returns: A `Srting` containing the value of the configuration variable
    */
    public func get(_ variable: String) throws -> String {
        
        guard let value = properties?[variable] else {
            throw KafkaError.configVariableNotFound(variable)
        }
        
        return value
        
    }
    
    /**
     Sets a value to a configuration variable
     - parameter variable: The name of the variable to set the value for
     - parameter value: The value of the configuration variable to set
    */
    public func set(_ variable: String, value: String) throws {
        
        let errStr = UnsafeMutablePointer<Int8>.allocate(capacity: kSwiftKafkaCStringSize)
        let result = rd_kafka_conf_set(configHandle, variable, value, errStr, kSwiftKafkaCStringSize)
        
        defer {
            errStr.deallocate(capacity: kSwiftKafkaCStringSize)
        }
        
        guard result == RD_KAFKA_CONF_OK else {
            
            let errorDesc = String(cString: errStr)
            
            if result == RD_KAFKA_CONF_INVALID {
                throw KafkaError.setConfigurationPropertyError(errorDesc)
            }
            
            if result == RD_KAFKA_CONF_UNKNOWN {
                throw KafkaError.setConfigurationPropertyError(errorDesc)
            }
            
            return
            
        }
    }
    
    public func configureOffsetCommitCallback() {
        
        rd_kafka_conf_set_offset_commit_cb(configHandle) { ptr, rawError, rawPartitions, hdlr in
            
            guard let rawPartitions = rawPartitions else {
                return
            }
            
            var notification: KafkaConsumerNotification
            
            let nc = NotificationCenter.default
            let partitions = KafkaTopicPartition.partitions(fromCPartitionsList: rawPartitions)
            
            if rawError == RD_KAFKA_RESP_ERR_NO_ERROR {
                notification = KafkaConsumerNotification(notificationType: .offsetsCommitted,
                                                         partitions: partitions)
            } else {
                
                let error = KafkaError.coreError(KafkaCoreError(rdError: rawError))
                
                notification = KafkaConsumerNotification(notificationType: .offsetsCommitted,
                                                         partitions: partitions,
                                                         error: error)
            
            }
            
            nc.post(name: NSNotification.Name(String(cString: rd_kafka_name(ptr))),
                    object: nil,
                    userInfo: notification.toDict())
        }
        
    }
    
    public func configureRebalanceCallback() {
        
        rd_kafka_conf_set_rebalance_cb(configHandle) { ptr, rawError, rawPartitions, _ in
        
            guard let rawPartitions = rawPartitions else {
                return
            }
            
            let nc = NotificationCenter.default
            let partitions = KafkaTopicPartition.partitions(fromCPartitionsList: rawPartitions)
            var notification: KafkaConsumerNotification
            
            if rawError == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
                notification = KafkaConsumerNotification(notificationType: .partitionsAssigned,
                                                         partitions: partitions)
            } else if rawError == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS {
                notification = KafkaConsumerNotification(notificationType: .partitionsRevoked,
                                                         partitions: partitions)
            } else {
                // TODO: Check whether `rawError` could be something meaningful
                return
            }
            
            nc.post(name: NSNotification.Name(String(cString: rd_kafka_name(ptr))),
                    object: nil,
                    userInfo: notification.toDict())
            
        }
        
    }
    
    public func configureMessageCallback() {
        
        rd_kafka_conf_set_dr_msg_cb(configHandle) { ptr, rawMessage, _ in
            
            guard let rawMessage = rawMessage else {
                return
            }
            
            let nc = NotificationCenter.default
            
            let message = KafkaMessage.message(fromRawMessage: rawMessage.pointee)
            
            let notification = KafkaProducerNotification(notificationType: .messageReceived,
                                                         message: message,
                                                         error: message?.error)
            
            nc.post(name: NSNotification.Name(String(cString: rd_kafka_name(ptr))),
                    object: nil,
                    userInfo: notification.toDict())

        }

    }
    
    // MARK: - Private Methods
    
    /**
     Returns a dictionary containing all the properties
     and values of the current configuration
    */
    private func listConfigurationProperties() -> [String: String]? {
        
        var properties: [String: String] = [:]
        var propertiesCount = 0
        
        guard let d = rd_kafka_conf_dump(configHandle, &propertiesCount), propertiesCount > 0 else {
            return nil
        }
        
        defer {
            rd_kafka_conf_dump_free(d, propertiesCount)
        }
        
        for i in 0...propertiesCount / 2 {
            
            guard let key = d.advanced(by: i*2).pointee,
                  let value = d.advanced(by: i*2 + 1).pointee else
            {
                break
            }
            
            properties[String(cString: key)] = String(cString: value)
            
        }
        
        return properties
        
    }
    
}
