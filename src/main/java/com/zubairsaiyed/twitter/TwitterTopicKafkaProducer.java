package com.zubairsaiyed.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterTopicKafkaProducer {

private static final Logger logger = LoggerFactory.getLogger(TwitterTopicKafkaProducer.class);
private static final String topic = "twitter-topic";
private static Properties prop;

public static void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {

        // Create Kafka producer instance
        Properties properties = new Properties();
        properties.put("metadata.broker.list", prop.getProperty("kafka_host")+":"+prop.getProperty("kafka_port"));
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>( producerConfig);

        // Blocking queue to buffer incoming tweets
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

        // Filter terms for Twitter stream
        List<String> terms = Lists.newArrayList("trump", "hillary", "bernie", "brexit");

        // Define Twitter endpoint options to enable term filtering
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.stallWarnings(false);
        endpoint.trackTerms(terms);

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

        // Define Twitter client details
        BasicClient client = new ClientBuilder()
                             .name("twitterTrackerClient")
                             .hosts(Constants.STREAM_HOST)
                             .endpoint(endpoint)
                             .authentication(auth)
                             .processor(new StringDelimitedProcessor(queue))
                             .build();

        // Establish a connection
        client.connect();

        // Get tweets from Twitter and pass as messages to Kafka
        while(true) {
                if (client.isDone()) {
                        logger.debug("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
                        break;
                }

                String msg = queue.poll(5, TimeUnit.SECONDS);
                if (msg == null) {
                        logger.debug("Did not receive a message in 5 seconds");
                } else {
                        logger.debug(msg);
                        KeyedMessage<String, String> message = null;
                        try {
                                message = new KeyedMessage<String, String>(topic, queue.take());
                        } catch (InterruptedException e) {
                                e.printStackTrace();
                        }
                        producer.send(message);
                }
        }
}

public static void main(String[] args) {
        prop = new Properties();
        try (InputStream input = TwitterTopicKafkaProducer.class.getClassLoader().getResourceAsStream("config.properties")) {
                prop.load(input);
                TwitterTopicKafkaProducer.run(prop.getProperty("twitter_api_key"),prop.getProperty("twitter_api_secret"),prop.getProperty("twitter_access_token"),prop.getProperty("twitter_access_token_secret"));
        } catch (Exception ex) {
                ex.printStackTrace();
        }
}
}
