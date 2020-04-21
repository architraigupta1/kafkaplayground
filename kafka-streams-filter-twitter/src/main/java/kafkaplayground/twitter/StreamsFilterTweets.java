package kafkaplayground.twitter;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
  public static void main(String[] args) {
    //create properties
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-twitter");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

    //create topology
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    //input topic
    KStream<String, String> inputTopic = streamsBuilder.stream("twitter-tweets");
    KStream<String, String> filteredStream = inputTopic.filter(
        (k, jsonTweet) -> extractUserFollowersFromTweet(jsonTweet) > 10000
    );

    filteredStream.to("important_tweets");

    //build topology
    KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

    //start our streams application
    kafkaStreams.start();
  }

  private static int extractUserFollowersFromTweet(String tweet) {
    try {
      return JsonParser
          .parseString(tweet)
          .getAsJsonObject()
          .get("user")
          .getAsJsonObject()
          .get("followers_count")
          .getAsInt();
    } catch (NullPointerException e) {
      return 0;
    }

  }
}
