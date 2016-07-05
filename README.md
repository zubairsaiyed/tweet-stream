# twitter-stream

twitter-stream is a sub-module of the [TwtrTrkr project](https://github.com/zubairsaiyed/TwtrTrkr). It contains two Kafka producers that connect to the Twitter Streaming API. The producers are listed below.

* TwitterKafkaProducer
 * Connects directly to Twitter Streaming API
* TwitterTopicKafkaProducer
 * Connects to Twitter Streaming API but also filters by pre-defined keywords

Additionally, a SampleStreamExample is contained which simply validates Twitter Streaming API credentials and prints received tweets to stdout (without connecting to Kafka).

## Requirements :

* Apache Kafka v0.8.1.1
* Twitter Developer account (for API Key, Secret etc.)
* Apache Maven
* Oracle JDK 1.7 (64 bit)

## Running

tweet-stream must first be configured with the Twitter API authentication credentials and Apache Kafka  details via a configuration file at `resources\config.properties`.

* Run `mvn install` to build project
* Run `.\run.sh` to launch both producers using NOHUP so they will run de-tached from the terminal session (allowing you to safely close the calling terminal session without terminating the producers)
