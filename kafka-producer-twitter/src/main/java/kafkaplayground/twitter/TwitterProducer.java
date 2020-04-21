package kafkaplayground.twitter;

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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

  private final String consumerKey;
  private final String consumerSecret;
  private final String token;
  private final String tokenSecret;
  List<String> terms;

  Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

  public TwitterProducer() {
    this.consumerKey = "09HEgU0NqGg2fd7KGhmb2vSVf";
    this.consumerSecret = "A6q3AxUReRpZOMnSlihElRmje2sDWjH1uVgmzoOqg3tN5kcToA";
    this.token = "1249094221560250370-uG0UNPjoNgdjRyiXx8TLK6VXVMM1e2";
    this.tokenSecret = "N44SELR9ZPb7TdAA6GKhWkpRbtaizSjBUhxINlp2QRIjL";
    this.terms = Lists.newArrayList("bitcoin", "usa", "sports", "politics", "coronavirus");
  }

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public Client getclient() {
    Client client = createTwitterClient(null);
    return client;
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
    KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Shutting down application");
      logger.info("Closing twitter connection");
      client.stop();
      logger.info("Shutting down kafka producer");
      kafkaProducer.close();
      logger.info("Done!");
    }));

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
        kafkaProducer.send(new ProducerRecord<>("twitter-tweets", null, msg), (recordMetadata, e) -> {
          if (e != null) {
            logger.error("Something bad happened", e);
          }
        });
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

  private KafkaProducer<String, String> createKafkaProducer() {
    String bootstrapServer = "127.0.0.1:9092";

    //create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create a safe producer
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

    // high throughput producer (at the expense of a bit of latency and cpu cycles)
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
    //create producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    return producer;
  }
}
