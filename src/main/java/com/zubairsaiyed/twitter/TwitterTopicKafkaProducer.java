package com.zubairsaiyed.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Properties;
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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterTopicKafkaProducer {

  private static final Logger logger = LoggerFactory.getLogger(TwitterTopicKafkaProducer.class);
  private static final String topic = "twitter-topic";
  private static Properties prop;

  public static void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", prop.getProperty("kafka_host")+":"+prop.getProperty("kafka_port"));
    properties.put("serializer.class", "kafka.serializer.StringEncoder");
    ProducerConfig producerConfig = new ProducerConfig(properties);
    kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>( producerConfig);

    // Create an appropriately sized blocking queue
    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

    List<String> terms = Lists.newArrayList("trump", "hillary", "bernie", "brexit");

    // Define our endpoint: By default, delimited=length is set (we need this for our processor)
    // and stall warnings are on.
    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    endpoint.stallWarnings(false);
    endpoint.trackTerms(terms);

    Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
    //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);


    // Create a new BasicClient. By default gzip is enabled.
    BasicClient client = new ClientBuilder()
            .name("twitterTrackerClient")
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(queue))
            .build();

    // Establish a connection
    client.connect();

    // Do whatever needs to be done with messages
    //for (int msgRead = 0; msgRead < 1000; msgRead++) {
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

    producer.close();
    client.stop();

    // Print some stats
    System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
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
