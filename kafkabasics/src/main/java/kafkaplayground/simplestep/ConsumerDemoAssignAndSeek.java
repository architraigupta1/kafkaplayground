package kafkaplayground.simplestep;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

  public static void main(String[] args) {

    Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);
    String bootstrapServers = "127.0.0.1:9092";
    String topic = "first_topic";

    //create consumer properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    //create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    //Assign a topic and partition to read from
    TopicPartition topicPartition = new TopicPartition(topic, 0);
    consumer.assign(Arrays.asList(topicPartition));

    //Seek to specific offset to read from
    long startOffset = 31L;
    consumer.seek(topicPartition, startOffset);

    //read only 5 messages
    int numberOfMessagesToRead = 5;
    int messagesReadSoFar = 0;
    boolean readingMessages = true;
    //poll for new data
    while(readingMessages) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : records) {
        messagesReadSoFar++;
        logger.info("Key: " + record.key() + " Value: " + record.value());
        logger.info("Topic: {}, Partition: {}, Offset: {}", record.topic(), record.partition(), record.offset());
        if (messagesReadSoFar >= numberOfMessagesToRead) {
          readingMessages = false;
          break;
        }
      }
    }


  }
}
