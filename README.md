# SwiftKafka
A *Swifty* wrapper for `librdkafka` for consuming and/or producing messages from/to Apache Kafka. The high level API was modeled around [Confluent's Python Kafka Client](https://github.com/confluentinc/confluent-kafka-python). Confluent is also the author of `librdkafka`.
<p>
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Swift-3.0-orange.svg?style=flat" alt="Swift 3.0">
    </a>
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Platforms-OS%20X%20%7C%20Linux%20-lightgray.svg?style=flat" alt="Platforms OS X | Linux">
    </a>
    <a href="http://perfect.org/licensing.html" target="_blank">
        <img src="https://img.shields.io/badge/License-MIT-lightgrey.svg?style=flat" alt="License MIT">
    </a>
    <a href="http://twitter.com/attheodo" target="_blank">
        <img src="https://img.shields.io/badge/Twitter-@attheodo-blue.svg?style=flat" alt="attheodo Twitter">
    </a>
</p>

<img src="https://photos-2.dropbox.com/t/2/AAApXdGdeOLwHS8cW-a3QjTo5cRJP7OKdl0b3r1Z_sw_Hw/12/1237004/png/32x32/3/1503856800/0/2/logo.png/EIXuhwEYnI2-7QMgAigCKAQ/_xuoBYEWM4X5XhwcDDkcd_TGYfSAggpmhKQxU22FI6E?dl=0&size=2048x1536&size_mode=3" alt="ATHSwift">

## TODO
- [ ] Provide usage examples
- [ ] Write API documentation
- [ ] Write tests

## Installation

**SwiftKafka** is available through the [Swift Package Manager](https://swift.org/package-manager/).

Before importing this package, please make sure you have installed `librdkafka` first:
- **Linux**
    - `$ sudo apt-get install librdkafka-dev`
- **macOS**
    - `$ brew install librdkafka`
    - Please also note that a proper pkg-config path setting is required:
        - `$ export PKG_CONFIG_PATH="/usr/local/lib/pkgconfig"`

## Author

Athanasios "attheodo" Theodoridis
- <a href="mailto:at@atworks.gr">at@atworks.gr</a>
- <a href="http://attheo.do">Personal website</a>

## License

**SwiftKafka** is available under the MIT license. See the LICENSE file for more info.

## Changelog
- **v0.1.0**, *Aug 2017*
    - Initial release
