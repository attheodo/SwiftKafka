//
//  OffsetPosition.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 27/08/2017.
//
//

import ckafka

public enum OffsetPosition {
    
    /// Start consuming from the beginning of
    /// Kafka partition queue (oldest msg)
    case beginning
    
    /// Start consuming from the end of
    ///Kafka partition queue (next msg)
    case end
    
    /// Start consuming from the offset retrieved
    /// from the offset store
    case stored
    
    case invalid
    
    /// Start consuming `offset` messages from
    /// the topic's current offset
    /// - If current offset is 1000 and `offset` is `10`, it will start
    /// consuming from `1000 - 10 = 990`
    case tail(Int64)
    
    /// Start consuming from the specified offset
    case specific(Int64)
    
    func value() -> Int64 {
        
        switch self {
            
        case .beginning:
            return Int64(RD_KAFKA_OFFSET_BEGINNING)
            
            
        case .invalid:
            return Int64(RD_KAFKA_OFFSET_INVALID)
            
        case .end:
            return Int64(RD_KAFKA_OFFSET_END)
            
        case .stored:
            return Int64(RD_KAFKA_OFFSET_STORED)
            
        case .tail(let offset):
            return (Int64(RD_KAFKA_OFFSET_TAIL_BASE) - offset)
            
        case .specific(let offset):
            return offset
        }
        
    }
    
}
