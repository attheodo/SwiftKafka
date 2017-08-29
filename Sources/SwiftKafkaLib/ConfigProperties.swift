//
//  ConfigProperties.swift
//  SwiftKafka
//
//  Created by Athanasios Theodoridis on 26/08/2017.
//
//

public enum DebugContext: String {
    
    case generic
    case broker
    case topic
    case metadata
    case producer
    case queue
    case msg
    case `protocol`
    case cgrp
    case security
    case fetch
    case all
    
}

public enum BrokerAddressFamily: String {
    
    case any  = "any"
    case ipv4 = "v4"
    case ipv6 = "v6"
    
}

public enum CompressionCodec: String {
    
    case none    = "none"
    case inherit = "inherit"
    case gzip    = "gzip"
    case snappy  = "snappy"
    
}

public enum AutoOffsetResetAction: String {
    
    case smallest = "smallest"
    case earliest = "earliest"
    case largest  = "largest"
    case latest   = "latest"
    case error    = "error"
    
}

public enum OffsetCommitStoreMethod: String {
    
    case file   = "file"
    case broker = "broker"
    
}

public enum GlobalConfigProperty {
    
    /// Client identifier. 
    /// - default: "librdkafka"
    case clientId(String)
    
    /// Initial list of brokers.
    case metadataBrokerList(String)
    
    /// Alias for `metadataBrokerList`
    case bootstrapServers(String)
    
    /// Maximum transmit message size
    /// - default: 1000000
    case messageMaxBytes(Int)
    
    /// Maximum receive message size. This is a safety precaution 
    /// to avoid memory exhaustion in case of protocol hickups. 
    /// The value should be at least fetch.message.max.bytes * number of partitions 
    /// consumed from + messaging overhead (e.g. 200000 bytes).
    /// - default: 100000000
    case receiveMessageMaxBytes(Int)
    
    /// Maximum number of in-flight requests the client will send. 
    /// This setting applies per broker connection.
    /// - default: 1000000
    case maxInFlightReqsPerConnection(Int)
    
    /// Non-topic request timeout in milliseconds. This is for metadata requests, etc.
    /// default: - 60000ms
    case metadataRequestTimeout(Int)
    
    /// Topic metadata refresh interval in milliseconds. The metadata is automatically 
    /// refreshed on error and connect. Use -1 to disable the intervalled refresh.
    /// default: 300000ms
    case topicMetadataRefreshInterval(Int)
    
    /// When a topic looses its leader this number of metadata requests are sent with 
    /// topic.metadata.refresh.fast.interval.ms interval disregarding the topic.metadata.refresh.interval.ms 
    /// value. This is used to recover quickly from transitioning leader brokers.
    /// - default: 10
    case topicMetadataRefreshFastCount(Int)
    
    /// When a topic looses its leader this number of metadata requests are sent with
    /// topic.metadata.refresh.fast.interval.ms interval disregarding the topic.metadata.refresh.interval.ms
    /// value. This is used to recover quickly from transitioning leader brokers.
    /// - default: 250ms
    case topicMetadataRefreshFastInterval(Int)
    
    /// Sparse metadata requests (consumes less network bandwidth)
    /// - default: `true`
    case topicMetadataRefreshSparse(Bool)
    
    /// Topic blacklist, a comma-separated list of regular expressions for 
    /// matching topic names that should be ignored in broker metadata information as if the topics did not exist.
    case topicBlackList(String)
    
    /// A comma-separated list of debug contexts to enable.
    case debug(DebugContext)
    
    /// Timeout for network requests. 
    /// - default: 60000
    case socketTimeout(Int)
    
    /// Maximum time a broker socket operation may block. 
    /// A lower value improves responsiveness at the expense of slightly higher CPU usage.
    /// - default: 100
    case socketBlockingMax(Int)
    
    /// Broker socket send buffer size. System default is used if 0.
    /// - default: 0
    case socketSendBufferBytes(Int)
    
    /// Broker socket receive buffer size. System default is used if 0.
    // - default: 0
    case socketReceiveBufferBytes(Int)
    
    /// Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets
    // - default: `false`
    case socketKeepaliveEnable(Bool)
    
    /// Disconnect from broker when this number of send failures (e.g., timed out requests) is reached. 
    /// Disable with 0. NOTE: The connection is automatically re-established.
    /// - default: 3
    case socketMaxFails(Int)
    
    /// How long to cache the broker address resolving results (milliseconds).
    /// - default: 1000
    case brokerAddressTTL(Int)
    
    /// Allowed broker IP address families: any, v4, v6
    /// - default: `any`
    case brokerAddressFamily(BrokerAddressFamily)
    
    /// Throttle broker reconnection attempts by this value +-50%.
    /// - default: 500
    case reconnectBackoffJitter(Int)
    
    /// `librdkafka` statistics emit interval. The application also needs to register a stats callback using `rd_kafka_conf_set_stats_cb()`. 
    /// The granularity is 1000ms. A value of 0 disables statistics.
    /// - default: 0
    case statisticsInterval(Int)
    
    /// Logging level (syslog(3) levels) 
    /// - default: 6
    case logLevel(Int)
    
    /// Print internal thread name in log messages (useful for debugging librdkafka internals)
    /// - default: `false`
    case logThreadName(Bool)
    
    /// Log broker disconnects. It might be useful to turn this off when interacting with 
    /// 0.9 brokers with an aggressive connection.max.idle.ms value.
    /// - default: `true`
    case logConnectionClose(Bool)
    
    /// Signal that librdkafka will use to quickly terminate on rd_kafka_destroy(). If this signal is not set
    /// then there will be a delay before `rd_kafka_wait_destroyed()` returns true as internal threads are timing out 
    /// their system calls. If this signal is set however the delay will be minimal. 
    /// The application should mask this signal as an internal signal handler is installed.
    /// - default: 0
    case internalTerminationSignal(Int)
    
    /// Request broker's supported API versions to adjust functionality to available protocol features. 
    /// If set to `false` the fallback version broker.version.fallback will be used. 
    /// NOTE: Depends on broker version >=0.10.0. If the request is not supported by (an older) broker the 
    /// broker.version.fallback fallback is used.
    /// - default: `false`
    case apiVersionRequest(Bool)
    
    /// Dictates how long the broker.version.fallback fallback is used in the case the ApiVersionRequest fails. 
    /// NOTE: The ApiVersionRequest is only issued when a new connection to the broker is made (such as after an upgrade).
    /// - default: 1200000
    case apiVersionFallback(Int)
    
    /// Older broker versions (<0.10.0) provides no way for a client to query for supported protocol 
    /// features (ApiVersionRequest, see api.version.request) making it impossible for the client to know
    /// what features it may use. As a workaround a user may set this property to the expected broker version and 
    /// the client will automatically adjust its feature set accordingly if the ApiVersionRequest fails (or is disabled). 
    /// The fallback broker version will be used for api.version.fallback.ms. 
    /// - Valid values are: 0.9.0, 0.8.2, 0.8.1, 0.8.0.
    /// - default: "0.9.0"
    case brokerVersionFallback(String)
    
    /// Protocol used to communicate with brokers.
    case securityProtocol(BrokerProtocol)
    
    /// A cipher suite is a named combination of authentication, encryption, MAC and key exchange algorithm used 
    /// to negotiate the security settings for a network connection using TLS or SSL network protocol. 
    /// - See manual page for ciphers(1) and `SSL_CTX_set_cipher_list(3).
    case sslCipherSuites(String)
    
    /// Path to client's private key (PEM) used for authentication.
    case sslKeyLocation(String)
    
    /// Private key passphrase
    case sslKeyPassword(String)
    
    /// Path to client's public key (PEM) used for authentication.
    case sslCertificateLocation(String)
    
    /// File or directory path to CA certificate(s) for verifying the broker's key.
    case sslCALocation(String)
    
    /// Path to CRL for verifying broker's certificate validity.
    case sslCRLLocation(String)
    
    /// SASL mechanism to use for authentication. 
    /// - Supported: GSSAPI, PLAIN
    /// - default: "GSSAPI"
    case saslMechanism(String)
    
    /// Kerberos principal name that Kafka runs as.
    /// - default: "kafka"
    case saslKerberosServiceName(String)
    
    /// This client's Kerberos principal name.
    /// - default: "kafkaclient"
    case saslKerberosPrincipal(String)
    
    /// Full kerberos kinit command string, %{config.prop.name} is replaced by corresponding 
    /// config object value, %{broker.name} returns the broker's hostname.
    case saslKerberosKinitCmd(String)
    
    /// Path to Kerberos keytab file. Uses system default if not set.
    /// - **NOTE**: This is not automatically used but must be added to the template in
    /// sasl.kerberos.kinit.cmd as ... -t %{sasl.kerberos.keytab}.
    case saslKerberosKeytab(String)
    
    /// Minimum time in milliseconds between key refresh attempts.
    /// - default: 60000
    case saslKerberosMinTimeBeforeRelogin(Int)
    
    /// SASL username for use with the PLAIN mechanism
    case saslUsername(String)
    
    /// SASL password for use with the PLAIN mechanism
    case saslPassword(String)

    /// Client group id string. All clients sharing the same group.id belong to the same group.
    case groupId(String)
    
    /// Name of partition assignment strategy to use when elected group leader assigns partitions to group members.
    /// default: "range,roundrobin"
    case partitionAssignmentStrategy(String)
    
    /// Client group session and failure detection timeout.
    /// - default: 30000
    case sessionTimeout(Int)
    
    /// Group session keepalive heartbeat interval.
    /// - default: 1000
    case heartbeatInterval(Int)
    
    /// Group protocol type
    /// - default: "consumer"
    case groupProtocolType(String)
    
    /// How often to query for the current client group coordinator. If the currently assigned coordinator 
    /// is down the configured query interval will be divided by ten to more quickly recover in 
    /// case of coordinator reassignment.
    /// - default: 600000
    case coordinatorQueryInterval(String)
    
    /// Automatically and periodically commit offsets in the background.
    /// - default: `true`
    case enableAutoCommit(Bool)
    
    /// The frequency in milliseconds that the consumer offsets are committed (written) to offset storage. (0 = disable)
    /// - default: 5000
    case autoCommitInterval(Int)
    
    /// Automatically store offset of last message provided to application.
    /// - default: `true`
    case enableAutoOffsetStore(Bool)
    
    /// Minimum number of messages per topic+partition in the local consumer queue.
    /// - default: 100000
    case queuedMinMessages(Int)
    
    /// Maximum number of kilobytes per topic+partition in the local consumer queue. 
    /// This value may be overshot by fetch.message.max.bytes.
    /// - default: 1000000
    case queuedMaxMessagesKbytes(Int)
    
    /// Maximum time the broker may wait to fill the response with fetch.min.bytes.
    /// - default: 100
    case fetchWaitMax(Int)
    
    /// Maximum number of bytes per topic+partition to request when fetching messages from the broker.
    /// - default: 1048576
    case fetchMessageMaxBytes(Int)
    
    /// Alias for fetchMessageMaxBytes
    case maxPartitionFetchBytes(Int)
    
    /// Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated 
    /// data will be sent to the client regardless of this setting.
    /// - default: 1
    case fetchMinBytes(Int)
    
    /// How long to postpone the next fetch request for a topic+partition in case of a fetch error.
    /// - default: 500
    case fetchErrorBackoff(Int)
    
    /// Offset commit store method: 
    /// - `file` - local file store (offset.store.path, et.al),
    /// - `broker` - broker commit store (requires Apache Kafka 0.8.2 or later on the broker).
    ///
    /// -- default: `broker`
    case offsetStoreMethod(OffsetCommitStoreMethod)
    
    /// Maximum number of messages allowed on the producer queue.
    /// - default: 100000
    case queueBufferingMaxMessages(Int)
    
    /// Maximum time, in milliseconds, for buffering data on the producer queue.
    /// - default: 1000
    case queueBufferingMax(Int)
    
    /// How many times to retry sending a failing MessageSet. **Note**: retrying may cause reordering.
    /// - default: 2
    case messageSendMaxRetries(Int)
    
    /// Alias for `messageSendMaxRetries`
    case retries(Int)
    
    /// The backoff time in milliseconds before retrying a message send.
    /// - default: 100
    case retryBackoff(Int)
    
    /// Compression codec to use for compressing message sets: `none`, `gzip` or `snappy`
    /// - default: `none`
    case compressionCodec(CompressionCodec)
    
    /// Maximum number of messages batched in one MessageSet.
    /// - default: 1000
    case batchNumMessages(Int)
    
    /// Only provide delivery reports for failed messages.
    /// - default: `false`
    case deliveryReportOnlyError(Bool)
    
    public var key: String {
        
        switch self {
        case .clientId(_):
            return "client.id"
        case .metadataBrokerList(_):
            return "metadata.broker.list"
        case .bootstrapServers(_):
            return "bootstrap.servers"
        case .messageMaxBytes(_):
            return "message.max.bytes"
        case .receiveMessageMaxBytes(_):
            return "receive.message.max.bytes"
        case .maxInFlightReqsPerConnection(_):
            return "max.in.flight.requests.per.connection"
        case .metadataRequestTimeout(_):
            return "metadata.request.timeout.ms"
        case .topicMetadataRefreshInterval(_):
            return "topic.metadata.refresh.interval.ms"
        case .topicMetadataRefreshFastCount(_):
            return "topic.metadata.refresh.fast.cnt"
        case .topicMetadataRefreshFastInterval(_):
            return "topic.metadata.refresh.fast.interval.ms"
        case .topicMetadataRefreshSparse(_):
            return "topic.metadata.refresh.sparse"
        case .topicBlackList(_):
            return "topic.blacklist"
        case .debug(_):
            return "debug"
        case .socketTimeout(_):
            return "socket.timeout.ms"
        case .socketBlockingMax(_):
            return "socket.blocking.max.ms"
        case .socketSendBufferBytes(_):
            return "socket.send.buffer.bytes"
        case .socketReceiveBufferBytes(_):
            return "socket.receive.buffer.bytes"
        case .socketKeepaliveEnable(_):
            return "socket.keepalive.enable"
        case .socketMaxFails(_):
            return "socket.max.fails"
        case .brokerAddressTTL(_):
            return "broker.address.ttl"
        case .brokerAddressFamily(_):
            return "broker.address.family"
        case .reconnectBackoffJitter(_):
            return "reconnect.backoff.jitter.ms"
        case .statisticsInterval(_):
            return "statistics.interval.ms"
        case .logLevel(_):
            return "log_level"
        case .logThreadName(_):
            return "log.thread.name"
        case .logConnectionClose(_):
            return "log.connection.close"
        case .internalTerminationSignal(_):
            return "internal.termination.signal"
        case .apiVersionRequest(_):
            return "api.version.request"
        case .apiVersionFallback(_):
            return "api.version.fallback.ms"
        case .brokerVersionFallback(_):
            return "broker.version.fallback"
        case .securityProtocol(_):
            return "security.protocol"
        case .sslCipherSuites(_):
            return "ssl.cipher.suites"
        case .sslKeyLocation(_):
            return "ssl.key.location"
        case .sslKeyPassword(_):
            return "ssl.key.password"
        case .sslCertificateLocation(_):
            return "ssl.certificate.location"
        case .sslCALocation(_):
            return "ssl.ca.location"
        case .sslCRLLocation(_):
            return "ssl.crl.location"
        case .saslMechanism(_):
            return "sasl.mechanisms"
        case .saslKerberosServiceName(_):
            return "sasl.kerberos.service.name"
        case .saslKerberosPrincipal(_):
            return "sasl.kerberos.principal"
        case .saslKerberosKinitCmd(_):
            return "sasl.kerberos.kinit.cmd"
        case .saslKerberosKeytab(_):
            return "sasl.kerberos.keytab"
        case .saslKerberosMinTimeBeforeRelogin(_):
            return "sasl.kerberos.min.time.before.relogin"
        case .saslUsername(_):
            return "sasl.username"
        case .saslPassword(_):
            return "sasl.password"
        case .groupId(_):
            return "group.id"
        case .partitionAssignmentStrategy(_):
            return "partition.assignment.strategy"
        case .sessionTimeout(_):
            return "session.timeout.ms"
        case .heartbeatInterval(_):
            return "heartbeat.interval.ms"
        case .groupProtocolType(_):
            return "group.protocol.type"
        case .coordinatorQueryInterval(_):
            return "coordinator.query.interval.ms"
        case .enableAutoCommit(_):
            return "enable.auto.commit"
        case .autoCommitInterval(_):
            return "auto.commit.interval.ms"
        case .enableAutoOffsetStore(_):
            return "enable.auto.offset.store"
        case .queuedMinMessages(_):
            return "queued.min.messages"
        case .queuedMaxMessagesKbytes(_):
            return "queued.max.messages.kbytes"
        case .fetchWaitMax(_):
            return "fetch.wait.max.ms"
        case .fetchMessageMaxBytes(_):
            return "fetch.message.max.bytes"
        case .maxPartitionFetchBytes(_):
            return "max.partition.fetch.bytes"
        case .fetchMinBytes(_):
            return "fetch.min.bytes"
        case .fetchErrorBackoff(_):
            return "fetch.error.backoff.ms"
        case .offsetStoreMethod(_):
            return "offset.store.method"
        case .queueBufferingMaxMessages(_):
            return "queue.buffering.max.messages"
        case .queueBufferingMax(_):
            return "queue.buffering.max.ms"
        case .messageSendMaxRetries(_):
            return "message.send.max.retries"
        case .retries(_):
            return "retries"
        case .retryBackoff(_):
            return "retry.backoff.ms"
        case .compressionCodec(_):
            return "compression.codec"
        case .batchNumMessages(_):
            return "batch.num.messages"
        case .deliveryReportOnlyError(_):
            return "delivery.report.only.error"
        
        }
        
    }
    
    public var value: String {
        
        switch self {
        
        case .clientId(let s):
            return s
        case .metadataBrokerList(let s):
            return s
        case .bootstrapServers(let s):
            return s
        case .messageMaxBytes(let i):
            return String(i)
        case .receiveMessageMaxBytes(let i):
            return String(i)
        case .maxInFlightReqsPerConnection(let i):
            return String(i)
        case .metadataRequestTimeout(let i):
            return String(i)
        case .topicMetadataRefreshInterval(let i):
            return String(i)
        case .topicMetadataRefreshFastCount(let i):
            return String(i)
        case .topicMetadataRefreshFastInterval(let i):
            return String(i)
        case .topicMetadataRefreshSparse(let b):
            return b == true ? "true" : "false"
        case .topicBlackList(let s):
            return s
        case .debug(let d):
            return d.rawValue
        case .socketTimeout(let i):
            return String(i)
        case .socketBlockingMax(let i):
            return String(i)
        case .socketSendBufferBytes(let i):
            return String(i)
        case .socketReceiveBufferBytes(let i):
            return String(i)
        case .socketKeepaliveEnable(let b):
            return b == true ? "true" : "false"
        case .socketMaxFails(let i):
            return String(i)
        case .brokerAddressTTL(let i):
            return String(i)
        case .brokerAddressFamily(let f):
            return f.rawValue
        case .reconnectBackoffJitter(let i):
            return String(i)
        case .statisticsInterval(let i):
            return String(i)
        case .logLevel(let i):
            return String(i)
        case .logThreadName(let b):
            return b == true ? "true" : "false"
        case .logConnectionClose(let b):
            return b == true ? "true" : "false"
        case .internalTerminationSignal(let i):
            return String(i)
        case .apiVersionRequest(let b):
            return b == true ? "true" : "false"
        case .apiVersionFallback(let i):
            return String(i)
        case .brokerVersionFallback(let s):
            return s
        case .securityProtocol(let p):
            return p.rawValue
        case .sslCipherSuites(let s):
            return s
        case .sslKeyLocation(let s):
            return s
        case .sslKeyPassword(let s):
            return s
        case .sslCertificateLocation(let s):
            return s
        case .sslCALocation(let s):
            return s
        case .sslCRLLocation(let s):
            return s
        case .saslMechanism(let s):
            return s
        case .saslKerberosServiceName(let s):
            return s
        case .saslKerberosPrincipal(let s):
            return s
        case .saslKerberosKinitCmd(let s):
            return s
        case .saslKerberosKeytab(let s):
            return s
        case .saslKerberosMinTimeBeforeRelogin(let i):
            return String(i)
        case .saslUsername(let s):
            return s
        case .saslPassword(let s):
            return s
        case .groupId(let s):
            return s
        case .partitionAssignmentStrategy(let s):
            return s
        case .sessionTimeout(let i):
            return String(i)
        case .heartbeatInterval(let i):
            return String(i)
        case .groupProtocolType(let s):
            return s
        case .coordinatorQueryInterval(let s):
            return s
        case .enableAutoCommit(let b):
            return b == true ? "true" : "false"
        case .autoCommitInterval(let i):
            return String(i)
        case .enableAutoOffsetStore(let b):
            return b == true ? "true" : "false"
        case .queuedMinMessages(let i):
            return String(i)
        case .queuedMaxMessagesKbytes(let i):
            return String(i)
        case .fetchWaitMax(let i):
            return String(i)
        case .fetchMessageMaxBytes(let i):
            return String(i)
        case .maxPartitionFetchBytes(let i):
            return String(i)
        case .fetchMinBytes(let i):
            return String(i)
        case .fetchErrorBackoff(let i):
            return String(i)
        case .offsetStoreMethod(let s):
            return s.rawValue
        case .queueBufferingMaxMessages(let i):
            return String(i)
        case .queueBufferingMax(let i):
            return String(i)
        case .messageSendMaxRetries(let i):
            return String(i)
        case .retries(let i):
            return String(i)
        case .retryBackoff(let i):
            return String(i)
        case .compressionCodec(let c):
            return c.rawValue
        case .batchNumMessages(let i):
            return String(i)
        case .deliveryReportOnlyError(let b):
            return b == true ? "true" : "false"
        }
        
    }
    
}

public enum TopicConfigProperty {
    
    /// This field indicates how many acknowledgements the leader broker must receive from ISR brokers 
    /// before responding to the request: 0=Broker does not send any response/ack to client, 1=Only the leader broker will 
    /// need to ack the message, -1 or all=broker will block until message is committed by all in sync replicas (ISRs) or 
    /// broker's in.sync.replicas setting before sending response.
    /// - default: 1
    case requestRequiredACKs(Int)
    
    /// Alias for requestRequiredACKs
    case acks(Int)
    
    /// The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and 
    /// relies on request.required.acks being > 0.
    /// - default: 5000
    case requestTimeout(Int)
    
    /// Local message timeout. This value is only enforced locally and limits the time a produced message 
    /// waits for successful delivery. A time of 0 is infinite.
    /// - default: 300000
    case messageTimeout(Int)
    
    /// Report offset of produced message back to application. The application must be use the `dr_msg_cb` to retrieve 
    /// the offset from `rd_kafka_message_t.offset.`
    /// - default: `false`
    case produceOffsetReport(Bool)
    
    /// Compression codec to use for compressing message sets: none, gzip or snappy
    /// - default: `inherit`
    case compressionCodec(CompressionCodec)
    
    /// If true, periodically commit offset of the last message handed to the application. 
    /// This committed offset will be used when the process restarts to pick up where it left off. If false, 
    /// the application will have to call rd_kafka_offset_store() to store an offset (optional). 
    /// NOTE: This property should only be used with the simple legacy consumer, when using the high-level KafkaConsumer the 
    /// `global auto.commit.enable` property must be used instead. 
    /// NOTE: There is currently no zookeeper integration, offsets will be written to broker or local file 
    /// according to `offset.store.method`.
    /// - default: `true`
    case autoCommitEnable(Bool)
    
    /// Alias for `autoCommitEnable`
    case enableAutoCommit(Bool)
    
    /// The frequency in milliseconds that the consumer offsets are committed (written) to offset storage.
    /// - default: 60000
    case autoCommitInterval(Int)
    
    /// Action to take when there is no initial offset in offset store or the desired offset is out of range: 
    /// 'smallest','earliest' - automatically reset the offset to the smallest offset, 
    /// 'largest','latest' - automatically reset the offset to the largest offset, 
    /// 'error' - trigger an error which is retrieved by consuming messages and checking 'message->err'.
    /// - default: `largest`
    case autoOffsetReset(AutoOffsetResetAction)
    
    /// Path to local file for storing offsets. If the path is a directory a filename 
    /// will be automatically generated in that directory based on the topic and partition.
    case offsetStorePath(String)
    
    /// `fsync()` interval for the offset file, in milliseconds. 
    /// Use -1 to disable syncing, and 0 for immediate sync after each write.
    /// - default: -1
    case offsetStoreSyncInterval(Int)
    
    /// Offset commit store method: 
    /// - `file` - local file store (offset.store.path, et.al),
    /// - `broker` - broker commit store (requires "group.id" to be configured and Apache Kafka 0.8.2 or later on the broker.).
    /// -- default: `broker`
    case offsetStoreMethod(OffsetCommitStoreMethod)
    
    /// Maximum number of messages to dispatch in one `rd_kafka_consume_callback*()` call (0 = unlimited)
    /// - default: 0
    case consumeCallbackMaxMessages(Int)
    
    public var key: String {
        
        get {
            
            switch self {
            
            case .requestRequiredACKs(_):
                return "request.required.acks"
            case .acks(_):
                return "acks"
            case .requestTimeout(_):
                return "request.timeout.ms"
            case .messageTimeout(_):
                return "message.timeout.ms"
            case .produceOffsetReport(_):
                return "produce.offset.report"
            case .compressionCodec(_):
                return "compression.codec"
            case .autoCommitEnable(_):
                return "auto.commit.enable"
            case .enableAutoCommit(_):
                return "enable.auto.commit"
            case .autoCommitInterval(_):
                return "auto.commit.interval.ms"
            case .autoOffsetReset(_):
                return "auto.offset.reset"
            case .offsetStorePath(_):
                return "offset.store.path"
            case .offsetStoreSyncInterval(_):
                return "offset.store.sync.interval.ms"
            case .offsetStoreMethod(_):
                return "offset.store.method"
            case .consumeCallbackMaxMessages(_):
                return "consume.callback.max.messages"
                
            }
        }
        
    }
    
    public var value: String {
        
        get {
            
            switch self {
            
            case .requestRequiredACKs(let i):
                return String(i)
            case .requestTimeout(let i):
                return String(i)
            case .acks(let i):
                return String(i)
            case .messageTimeout(let i):
                return String(i)
            case .produceOffsetReport(let b):
                return b == true ? "true" : "false"
            case .compressionCodec(let c):
                return c.rawValue
            case .autoCommitEnable(let b):
                return b == true ? "true" : "false"
            case .enableAutoCommit(let b):
                return b == true ? "true" : "false"
            case .autoCommitInterval(let i):
                return String(i)
            case .autoOffsetReset(let a):
                return a.rawValue
            case .offsetStorePath(let s):
                return s
            case .offsetStoreSyncInterval(let i):
                return String(i)
            case .offsetStoreMethod(let m):
                return m.rawValue
            case .consumeCallbackMaxMessages(let i):
                return String(i)
                
            }
            
        }
        
    }
    
}

extension GlobalConfigProperty: CustomStringConvertible {
    
    public var description: String {
        return "[\(self.key)]=\(self.value)"
    }
    
}

extension TopicConfigProperty: CustomStringConvertible {
    
    public var description: String {
        return "[\(self.key)]=\(self.value)"
    }
    
}
