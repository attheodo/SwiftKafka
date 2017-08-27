//
//  MessageContainer.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 26/08/2017.
//
//

typealias MessageDeliveryClosure = (_ message: KafkaMessage?, _ error: KafkaError?) -> ()

/// Class for objects carried across `librdkafka`'s internals as pointers and used
/// to get references for callbacks and producer/consumer instances
internal class MessageContainer {
    
    /// The instance of the producer that produced this message
    public let producerInstance: KafkaProducer!
    
    /// The closure to be called when the message is delivered
    public let deliveryClosure: MessageDeliveryClosure?
    
    init(producerInstance: KafkaProducer, deliveryClosure: MessageDeliveryClosure? = nil) {
        
        self.producerInstance = producerInstance
        self.deliveryClosure = deliveryClosure
        
    }
    
}
