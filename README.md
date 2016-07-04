# twitter-stream

twitter-stream contains two Kafka producers that connect to the Twitter Streaming API. The producers are listed below.

* TwitterKafkaProducer
 * Connects directly to Twitter Streaming API
* TwitterTopicKafkaProducer
 * Connects to Twitter Streaming API but also filters by pre-defined keywords

Additionally, a SampleStreamExample is contained which simply validates Twitter Streaming API credentials (without connecting to Kafka).

## Requirements :

Apache Kafka 1.0.1
Twitter Developer account (for API Key, Secret etc.)
Apache Zookeeper (required for Kafka)
Apache Maven
Oracle JDK 1.7 (64 bit)

## Running

* First run `mvn install`
* Then run `.\run.sh`. This will launch both producers using NOHUP so they will run de-tached from the terminal session
