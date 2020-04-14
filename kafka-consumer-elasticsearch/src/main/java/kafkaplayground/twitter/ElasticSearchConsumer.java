package kafkaplayground.twitter;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

  public static void main(String[] args) throws IOException, InterruptedException {
    Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    RestHighLevelClient client = createClient();

    KafkaConsumer<String, String> consumer = createConsumer("twitter-tweets");

    //poll for new data
    while(true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      logger.info("Records received count : {}", records.count());

      BulkRequest bulkRequest = new BulkRequest();

      for (ConsumerRecord<String, String> record : records) {
        try {
          String id = extractIdFromTweet(record.value());
          IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
              .source(record.value(), XContentType.JSON);

          bulkRequest.add(indexRequest);
        } catch (NullPointerException ex) {
          logger.error("Bad data passed: {}", record.value());
        }
      }
      if (records.count() <= 0) {
        continue;
      }
      BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
      logger.info("committing offsets...");
      consumer.commitSync();
      logger.info("committed offsets");
      Thread.sleep(1000);
    }

    // To close the client gracefully.
    //client.close();

  }

  public static RestHighLevelClient createClient() {

    String host = "twitter-tweets-poc-2289837690.ap-southeast-2.bonsaisearch.net";
    String username = "187ylnb8yn";
    String password = "nrvi6upwvp";

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    RestClientBuilder builder = RestClient.builder(
        new HttpHost(host, 443, "https"))
        .setHttpClientConfigCallback((HttpAsyncClientBuilder httpAsyncClientBuilder) -> {
          return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        });

    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;

  }

  public static KafkaConsumer createConsumer(String topic) {
    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "kafka-elasticsearch";

    //create consumer properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

    //create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    //subscribe to topic(s)
    consumer.subscribe(Arrays.asList(topic));

    return consumer;
  }

  private static String extractIdFromTweet(String tweet) {
    return JsonParser
        .parseString(tweet)
        .getAsJsonObject()
        .get("id_str")
        .getAsString();

  }
}
