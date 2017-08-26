// swift-tools-version:3.1

import PackageDescription

let package = Package(
    name: "SwiftKafka",
    targets: [
        Target(
            name: "SwiftKafka",
            dependencies: ["SwiftKafkaLib"]
        ),
        Target(name: "SwiftKafkaLib")
    ],
    dependencies: [
        .Package(url: "https://github.com/PerfectlySoft/Perfect-libKafka.git", majorVersion: 1)
    ]
    
)
