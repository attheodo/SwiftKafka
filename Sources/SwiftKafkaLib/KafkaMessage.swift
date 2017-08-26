//
//  KafkaMessage.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 26/08/2017.
//
//

import Foundation

import ckafka

public struct KafkaMessage {
    
    // MARK: - Static
    
    /// Creates a `KafkaMessage` object from a raw `rd_kafka_message_t`
    public static func message(fromRawMessage message: rd_kafka_message_t) -> KafkaMessage?
    {
        
        var error: KafkaError? = nil
        var key: Data? = nil
        var value: Data? = nil
        
        // set the key if any
        if let keyPointer = message.key {
            
            let p = keyPointer.assumingMemoryBound(to: Int8.self)
            var keyData = Array(UnsafeBufferPointer<Int8>(start: p, count: message.key_len))
            
            key = Data(bytes: &keyData, count: keyData.count)
            
        }
        
        // set the value if any
        if let valuePointer = message.payload {
            
            let p = valuePointer.assumingMemoryBound(to: Int8.self)
            var valueData = Array(UnsafeBufferPointer<Int8>(start: p, count: message.len))
            
            value = Data(bytes: &valueData, count: valueData.count)
            
        }
        
        // set the error if any
        if message.err != RD_KAFKA_RESP_ERR_NO_ERROR {
            error = KafkaError.coreError(KafkaCoreError(message.err))
        }
        
        
        guard let topicPtr = message.rkt else {
            return nil
        }
        
        return KafkaMessage(topic: String(cString: rd_kafka_topic_name(topicPtr)),
                            key: key,
                            value: value,
                            partition: message.partition,
                            offset: message.offset,
                            error: error)
        
    }
    // MARK: - Public Properties
    
    /// The topic for this message
    public let topic: String?
    
    /// The partition of the message
    public let partition: Int32
    
    /// The offset of the message inside the topic
    public let offset: Int64
    
    /// Whether the message has an error or not
    public let error: KafkaError?
    
    /// The message key
    public let key: Data?
    
    /// The message value
    public let value: Data?
    
    /// The value of `key` as a String, if convertible
    public var keyString: String? {
        
        guard let key = self.key else {
            return nil
        }
        
        if let string = String(data: key, encoding: .utf8) {
            return string
        } else {
            return nil
        }
        
    }
    
    /// The value of `value` as a String, if convertible
    public var valueString: String? {
        
        guard let value = self.value else {
            return nil
        }
        
        if let string = String(data: value, encoding: .utf8) {
            return string
        } else {
            return nil
        }
        
    }
    
    // MARK: - Initialiser
    init(topic: String?, key: Data?, value: Data?, partition: Int32, offset: Int64, error: KafkaError?) {
        
        self.topic = topic
        self.key = key
        self.value = value
        self.partition = partition
        self.offset = offset
        self.error = error
        
    }
    
}

extension KafkaMessage: CustomStringConvertible {
    
    public var description: String {
        
        if let key = self.key, let value = self.value {
            
            if let keyString = self.keyString {
                
                if let valueString = self.valueString {
                    return "#\(offset) [\(topic ?? "UKNOWN_TOPIC")] - [\(keyString)]=\"\(valueString)\""
                } else {
                    return "#\(offset) [\(topic ?? "UKNOWN_TOPIC")] - [\(keyString)]=<data>(\(value.count))"
                }
                
            } else {
                
                if let valueString = self.valueString {
                    return "#\(offset) [\(topic ?? "UKNOWN_TOPIC")] - [<data>(\(key.count))]=\"\(valueString)\""
                } else {
                    return "#\(offset) [\(topic ?? "UKNOWN_TOPIC")] - [<data>(\(key.count))]=<data>(\(value.count))"
                }
                
            }
            
        } else if let value = self.value {
            
            if let valueString = self.valueString {
                return "#\(offset) [\(topic ?? "UKNOWN_TOPIC")] - [NO_KEY]=\"\(valueString)\""
            } else {
                return "#\(offset) [\(topic ?? "UKNOWN_TOPIC")] - [NO_KEY]=<data>(\(value.count))"
            }
            
        } else {
            return "#\(offset) [\(topic ?? "UKNOWN_TOPIC")] - [NO_KEY]=\"(NO_VALUE)\""
        }
        
    }
    
}
