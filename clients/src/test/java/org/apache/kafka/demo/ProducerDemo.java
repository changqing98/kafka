package org.apache.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {

  Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

  private final static String TOPIC = "test";

  @Test
  public void testProduce() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, "hello");
    try {
      RecordMetadata recordMetadata = producer.send(producerRecord).get();
      logger.info(recordMetadata.toString());
    } catch (InterruptedException | ExecutionException e) {
      logger.error(e.getMessage(), e);
    }
  }
}
