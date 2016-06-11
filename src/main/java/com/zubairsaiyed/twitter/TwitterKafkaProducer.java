package com.zubairsaiyed.twitter;

import java.util.Properties;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterKafkaProducer {

  private static final String topic = "twitter-topic";

  public static void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {

    Properties properties = new Properties();
    properties.put("metadata.broker.list", "localhost:9092");
    properties.put("serializer.class", "kafka.serializer.StringEncoder");
    properties.put("client.id","camus");
    ProducerConfig producerConfig = new ProducerConfig(properties);
    kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
    		producerConfig);

    // Create an appropriately sized blocking queue
    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

    // Define our endpoint: By default, delimited=length is set (we need this for our processor)
    // and stall warnings are on.
    StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
    endpoint.stallWarnings(false);

    Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
    //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

    // Create a new BasicClient. By default gzip is enabled.
    BasicClient client = new ClientBuilder()
            .name("sampleExampleClient")
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(queue))
            .build();

    // Establish a connection
    client.connect();

    // Do whatever needs to be done with messages
    for (int msgRead = 0; msgRead < 1000; msgRead++) {
      if (client.isDone()) {
        System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
        break;
      }

      String msg = queue.poll(5, TimeUnit.SECONDS);
      if (msg == null) {
        System.out.println("Did not receive a message in 5 seconds");
      } else {
        System.out.println(msg);
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
    try {
      TwitterKafkaProducer.run(args[0], args[1], args[2], args[3]);
    } catch (InterruptedException e) {
      System.out.println(e);
    }
  }
}
