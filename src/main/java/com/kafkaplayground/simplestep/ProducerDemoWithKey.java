package com.kafkaplayground.simplestep;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {

  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class);

    String bootstrapServer = "127.0.0.1:9092";

    //create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //create producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    for (int i = 0; i < 10; i++) {

      String key = "id_" + i;
      //create a producer record
      ProducerRecord<String, String> record = new ProducerRecord("first_topic", key,
          "hello world " + i);

      logger.info("\nkey: " + key);

      //send data - asynchronous
      producer.send(record, (recordMetadata, e) -> {
        if (e == null) {
          logger.info("Record metadata: \n" +
              "Topic: " + recordMetadata.topic() +
              "\nPartition: " + recordMetadata.partition() +
              "\nOffset: " + recordMetadata.offset() +
              "\nTimestamp: " + recordMetadata.timestamp());
        } else {
          logger.error("Error occured while producing", e);
        }
      });
    }

    //flush data
    producer.flush();

    //close producer
    producer.close();
  }
}
