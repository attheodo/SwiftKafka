//
//  KafkaError.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 26/08/2017.
//
//

import ckafka

public enum KafkaError: Error {
    
    /// When a configuration variable could not be found
    case configVariableNotFound(String)
    
    /// When a configuration variable is set to an invalid value
    case configVariableInvalidValue(String, String)
    
    /// When base Kafka initialisation fails
    case baseKafkaInitFailed(String)
    
    /// When something really shitty happens
    case unknownError
    
    /// Errors emerging from `librdkafka` operations
    case coreError(KafkaCoreError)
    
    func localizedDescription() -> String {
        
        switch self {
        case .configVariableNotFound(let variable):
            return "Configuration variable \"\(variable)\" not found"
        case .configVariableInvalidValue(let variable, let error):
            return "Invalid configuration value for \"\(variable)\": \(error)"
        case .baseKafkaInitFailed(let error):
            return "Error while trying to initialise base Kafka client: \(error)"
        case .unknownError:
            return "An unknown error occured (possibly lost reference to an important pointer)"
        case .coreError(let error):
            return error.localizedDescription
        }
        
    }
    
}

public struct KafkaCoreError: Error {
    
    // MARK: - Public Properties
    
    public private(set) var error: rd_kafka_resp_err_t
    
    // MARK: - Initialiser
    public init(_ error: rd_kafka_resp_err_t) {
        self.error = error
    }
    
    /// The error's enum name
    public var enumName: String {
        return String(cString: rd_kafka_err2name(error))
    }
    
    // MARK: - Overrides
    
    public var localizedDescription: String {
        return "[\(enumName)] \(String(cString: rd_kafka_err2str(error)))"
    }
}



