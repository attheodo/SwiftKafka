//
//  GlobalConfig.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 26/08/2017.
//
//

import ckafka

/// A struct used for creating and manipulating global Kafka
/// configurations
public struct GlobalConfig {
    
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
            
            guard let h = rd_kafka_conf_dup(config.handle) else {
                throw KafkaError.globalConfigDuplicationError
            }
            
            handle = h
            
        } else {
            
            guard let h = rd_kafka_conf_new() else {
                throw KafkaError.globalConfigCreationError
            }
            
            handle = h
            
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
