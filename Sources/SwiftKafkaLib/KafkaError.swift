//
//  KafkaError.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 26/08/2017.
//
//

public enum KafkaError: Error {
    
    /// When SwiftKafka fails to create a new global config
    case globalConfigCreationError
    
    /// When SwiftKafka fails to duplicate the current config
    case globalConfigDuplicationError
    
    /// When SwiftKafka fails to create a new topic config
    case topicConfigCreationError
    
    /// When SwiftKafka fails to duplicate the current topic config
    case topicConfigDuplicationError
    
    /// When a configuration variable could not be found
    case configVariableNotFound(String)
    
    /// When a configuration variable is set to an invalid value
    case configVariableInvalidValue(String, String)
    
    func localizedDescription() -> String {
        
        switch self {
        case .globalConfigCreationError:
            return "Could not create global Kafka configuration"
        case .globalConfigDuplicationError:
            return "Could not duplicate current global Kafka configuration"
        case .topicConfigCreationError:
            return "Could not create Kafka topic configuration"
        case .topicConfigDuplicationError:
            return "Could not duplicate current Kafka topic configuration"
        case .configVariableNotFound(let variable):
            return "Configuration variable \"\(variable)\" not found"
        case .configVariableInvalidValue(let variable, let error):
            return "Invalid configuration value for \"\(variable)\": \(error)"
        }
        
    }
    
}

