package com.kafkaplayground.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

  private final String consumerKey;
  private final String consumerSecret;
  private final String token;
  private final String tokenSecret;

  Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

  public TwitterProducer() {
    this.consumerKey = "09HEgU0NqGg2fd7KGhmb2vSVf";
    this.consumerSecret = "A6q3AxUReRpZOMnSlihElRmje2sDWjH1uVgmzoOqg3tN5kcToA";
    this.token = "1249094221560250370-uG0UNPjoNgdjRyiXx8TLK6VXVMM1e2";
    this.tokenSecret = "N44SELR9ZPb7TdAA6GKhWkpRbtaizSjBUhxINlp2QRIjL";
  }

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {
    logger.info("Setup");
    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue(1000);

    // Create a twitter client
    Client client = createTwitterClient(msgQueue);
    // Attempts to establish a connection.
    client.connect();
    //create a kafka producer

    // on a different thread, or multiple different threads....
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }

      if (msg != null) {
        logger.info(msg);
      }
    }

    logger.info("End of application");
    //loop to send tweets to kafka
  }

  private Client createTwitterClient(BlockingQueue<String> msgQueue) {

    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    // Optional: set up some followings and track terms

    List<String> terms = Lists.newArrayList("coronavirus");
    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

    ClientBuilder builder = new ClientBuilder()
        .name("archit-kafka-playground")                              // optional: mainly for the logs
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue));

    Client hosebirdClient = builder.build();
    return hosebirdClient;
  }
}
