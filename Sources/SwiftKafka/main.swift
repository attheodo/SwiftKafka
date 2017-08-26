import SwiftKafkaLib
import Foundation
import ckafka

do {
    
    let broker = BrokerConnection(withProtocol: .plaintext, host: "localhost", port: 9092)
    let config = try KafkaConfig()
    
    try config.set("bootstrap.servers", value: "localhost:9092")
    try config.set("group.id", value: "dad1sa3aa")
    try config.set("session.timeout.ms", value: "1000")
    
    var consumer = try KafkaConsumer(kafkaConfig: config)
    let partition = KafkaTopicPartition(topic: "test", partition: 0, offset: .beginning, error: nil)
    //try consumer.assign(topicPartitions: [partition])
    //print(consumer.assignment)
    try consumer.subscribe(toTopics: ["test"])
//    print(try consumer.committedOffsets(forTopicPartitions: [partition]))
//    print(try consumer.position(forTopicPartitions: [partition]))
    
    var i = 1000
    
    while(i > 0) {
        print("here")
        // TODO: Make poll return an enum with either the result, EOF, or error
        if let msg = consumer.poll(timeout: 100) {
            print(msg)
            //try(consumer.commit(messages: [msg], async: false))
        }
        
        
        i -= 1
    }
  //  print(try consumer.position(forTopicPartitions: [partition]))
    try(consumer.unsubscribe())
    consumer.close()
    exit(0)
    
} catch(let err){
    print(err)
}




