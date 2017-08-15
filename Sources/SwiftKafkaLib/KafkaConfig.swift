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
            
            var consumerInstance: KafkaConsumer
            let partitions = KafkaTopicPartition.partitions(fromCPartitionsList: rawPartitions)
            
            if let consumerRef = hdlr {
                consumerInstance = Unmanaged<KafkaConsumer>.fromOpaque(consumerRef).takeUnretainedValue()
            } else {
                return
            }
            
            guard let kafkaClientHandle = consumerInstance.kafkaClientHandle else {
                return
            }
            
            var error: KafkaError? = nil
            
            if rawError != RD_KAFKA_RESP_ERR_NO_ERROR {
                error = KafkaError.coreError(KafkaCoreError(rdError: rawError))
            }
            
            if let callback = consumerInstance.onOffsetsCommitClosure {
                callback(error, partitions)
            } else {
                rd_kafka_yield(kafkaClientHandle)
            }
            
        }
        
    }
    
    public func configureRebalanceCallback() {
        
        rd_kafka_conf_set_rebalance_cb(configHandle) { ptr, rawError, rawPartitions, hdlr in
            
            guard let rawPartitions = rawPartitions else {
                return
            }
            
            var consumerInstance: KafkaConsumer
            let partitions = KafkaTopicPartition.partitions(fromCPartitionsList: rawPartitions)
            
            if let consumerRef = hdlr {
                consumerInstance = Unmanaged<KafkaConsumer>.fromOpaque(consumerRef).takeUnretainedValue()
            } else {
                return
            }
            
            guard let kafkaClientHandle = consumerInstance.kafkaClientHandle else {
                return
            }
            
            if rawError == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
                
                if let assignClosure = consumerInstance.onAssignClosure {
                    assignClosure(partitions)
                } else {
                    rd_kafka_yield(kafkaClientHandle)
                }
                
            } else if rawError == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS {
                
                if let revokeClosure = consumerInstance.onRevokeClosure {
                    revokeClosure(partitions)
                } else {
                    rd_kafka_yield(kafkaClientHandle)
                }
                
            }
            
            /**
             librdkafka requires the rebalance callback to call `assign()` to sync state.
             If the user did not do this manually in the callback or there was no callback set
             then we handle that here
             */
            
            guard consumerInstance.rebalanceAssigned > 0 else {
                return
            }
            
            if rawError == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
                rd_kafka_assign(kafkaClientHandle, rawPartitions)
            } else {
                rd_kafka_assign(kafkaClientHandle, nil)
            }
            
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
