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
                throw SwiftKafkaError.configDuplicationError
            }
            
            configHandle = cfg
            
        } else {
            
            guard let cfg = rd_kafka_conf_new() else {
                throw SwiftKafkaError.configCreationError
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
            throw SwiftKafkaError.configVariableNotFound(variable)
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
                throw SwiftKafkaError.setConfigurationPropertyError(errorDesc)
            }
            
            if result == RD_KAFKA_CONF_UNKNOWN {
                throw SwiftKafkaError.setConfigurationPropertyError(errorDesc)
            }
            
            return
            
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
