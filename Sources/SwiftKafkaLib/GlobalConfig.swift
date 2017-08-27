//
//  GlobalConfig.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 26/08/2017.
//
//

import ckafka

/// A class used for creating and manipulating global Kafka
/// configurations
public class GlobalConfig {
    
    // MARK: - Public Properties
    
    /// The current configuration properties
    public var properties: [String: String]? {
        return configurationProperties()
    }
    
    /// The current config handle pointer
    public private(set) var handle: OpaquePointer
    
    // MARK: - Initialiser
    
    /// Initialises a new configuration object
    /// - parameter config: An instance of `GlobalConfig` to duplicate instead of creating 
    /// a new configuration with default properties.
    init(byDiplicatingConfig config: GlobalConfig? = nil) throws {
        
        if let config = config {
            handle = rd_kafka_conf_dup(config.handle)
        } else {
            handle = rd_kafka_conf_new()
        }
        
    }
    
    // MARK: - Public Methods
    
    /**
     Returns the value of a configuration variable
     - parameter variable: An enum representing the configuration variable
     - returns: A `String` containing the value of the configuration variable
     */
    public func get(_ variable: GlobalConfigProperty) throws -> String {
        return try get(variable.key)
    }
    
    /**
     Returns the value of a configuration variable
     - parameter variable: The name of the variable for which to get the value
     - returns: A `String` containing the value of the configuration variable
     */
    public func get(_ variable: String) throws -> String {
        
        guard let value = properties?[variable] else {
            throw KafkaError.configVariableNotFound(variable)
        }
        
        return value
        
    }
    
    /**
     Sets configuration variables to the global config
     - parameter variables: An array of configuration enums
     */
    public func set(_ variables: [GlobalConfigProperty]) throws {
        let _ = try variables.map({ try set($0) })
    }

    /**
     Sets a value to a configuration variable
     - parameter variable: A configuration enum variable
     */
    public func set(_ variable: GlobalConfigProperty) throws {
        try set(variable.key, value: variable.value)
    }
    
    /**
     Sets a value to a configuration variable
     - parameter variable: The name of the variable to set the value for
     - parameter value: The value of the configuration variable to set
     */
    public func set(_ variable: String, value: String) throws {
        
        let errStr = UnsafeMutablePointer<Int8>.allocate(capacity: kSwiftKafkaCStringSize)
        let result = rd_kafka_conf_set(handle, variable, value, errStr, kSwiftKafkaCStringSize)
        
        defer {
            errStr.deallocate(capacity: kSwiftKafkaCStringSize)
        }
        
        guard result == RD_KAFKA_CONF_OK else {
            
            let errorDesc = String(cString: errStr)
            
            if result == RD_KAFKA_CONF_INVALID {
                throw KafkaError.configVariableInvalidValue(variable, errorDesc)
            }
            
            if result == RD_KAFKA_CONF_UNKNOWN {
                throw KafkaError.configVariableNotFound(variable)
            }
            
            return
            
        }
    
    }
    
    /**
     Configures the callback to call when a message is delivered.
     This takes care calling both the top-level, producer callback, if any,
     and the message-level, unique callback, if any.
    */
    internal func configureMessageDeliveryCallback() {
        
        rd_kafka_conf_set_dr_msg_cb(handle) { ptr, rawMessage, opaque in
            
            guard let rawMessage = rawMessage,
                  let msgContainerRef = rawMessage.pointee._private else {
                return
            }
            
            var callbackCalls = 0
            
            let message = KafkaMessage.message(fromRawMessage: rawMessage.pointee)
            let messageContainer = Unmanaged<MessageContainer>.fromOpaque(msgContainerRef).takeRetainedValue()
            
            // top-level, producer's callback
            if let opaque = opaque {
                
                if let producer = Unmanaged<KafkaBase>.fromOpaque(opaque).takeUnretainedValue() as? KafkaProducer {
                    
                    if let callback = producer.onMessageDelivery {
                        callback(message, message?.error)
                        callbackCalls += 1
                    }
                }

            }
            
            // message-level callback
            if let deliveryClosure = messageContainer.deliveryClosure {
            
                callbackCalls += 1
                deliveryClosure(message, message?.error)
                
            }
            
            if callbackCalls == 0 {
                rd_kafka_yield(ptr)
            }
        
        }
        
    }
    
    /**
     Sets the client's opaque reference in order to be carried along
     in `librdkafka`'s callbacks
    */
    internal func setOpaquePointer(forKafkaClient client: KafkaBase) {
       
        let clientRef = UnsafeMutableRawPointer(Unmanaged.passRetained(client).toOpaque())
        
        rd_kafka_conf_set_opaque(handle, clientRef)
        rd_kafka_topic_conf_set_opaque(handle, clientRef)

    }
    
    /**
     Configures the callback to be executed when the consumer offsets have been commited
    */
    internal func configureOffsetCommitCallback() {
        
        rd_kafka_conf_set_offset_commit_cb(handle) { ptr, rawError, rawPartitions, opaque in
            
            guard let rawPartitions = rawPartitions, let opaque = opaque else {
                return
            }
            
            guard let consumer = Unmanaged<KafkaBase>.fromOpaque(opaque).takeUnretainedValue() as? KafkaConsumer else {
                return
            }
            
            guard let callback = consumer.onOffsetsCommit else {
                
                rd_kafka_yield(consumer.handle)
                return
                
            }
            
            let partitions = TopicPartition.partitions(fromPartitionList: rawPartitions)
            let error: KafkaError? = rawError != RD_KAFKA_RESP_ERR_NO_ERROR ? KafkaError.coreError(KafkaCoreError(rawError)) : nil
        
            callback(partitions, error)
            
        }
        
    }
    
    /**
     Configures the callback to be executed when a partition rebalancing occurs for the consumer
    */
    internal func configureRebalanceCallback() {
        
        rd_kafka_conf_set_rebalance_cb(handle) { ptr, rawError, rawPartitions, opaque in
            
            guard let rawPartitions = rawPartitions,
                  let opaque = opaque else
            {
                return
            }
            
            guard let consumer = Unmanaged<KafkaBase>.fromOpaque(opaque).takeUnretainedValue() as? KafkaConsumer else {
                return
            }
            
            consumer.resetRebalanceAssignment()
            
            var callbacksCalled = 0
            
            let partitions = TopicPartition.partitions(fromPartitionList: rawPartitions)
            
            if rawError == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
                
                if let callback = consumer.onPartitionAssign {
                    
                    callbacksCalled += 1
                    callback(partitions)
                    
                }
                
            } else if rawError == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS {
                
                if let callback = consumer.onPartitionRevoke {
                    
                    callbacksCalled += 1
                    callback(partitions)
                }
                
            }
            
            if callbacksCalled == 0 {
                rd_kafka_yield(consumer.handle)
            }
            
            /* 
             * Fallback: librdkafka needs the rebalance_cb to call assign()
             * to synchronize state, if the user did not do this from callback,
             * or there was no callback, or the callback failed, then we perform
             * that assign() call here instead. 
             */
            if consumer.rebalanceAssigned == 0 || callbacksCalled == 0 {
                
                if rawError == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
                    rd_kafka_assign(consumer.handle, rawPartitions)
                } else if rawError == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS {
                    rd_kafka_assign(consumer.handle, nil)
                }
                
            }
            
        }
        
    }
    
    // MARK: - Private Methods
    
    /**
     Returns a dictionary containing all the properties
     and values of the current configuration
     */
    private func configurationProperties() -> [String: String]? {
        
        var properties: [String: String] = [:]
        var propertiesCount = 0
        
        guard let d = rd_kafka_conf_dump(handle, &propertiesCount), propertiesCount > 0 else {
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
