package org.apache.kafka.demo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

public class ConsumerDemo {

  private static final String TOPIC = "test";

  @Test
  public void testConsume() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
    Consumer<String, String> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singleton(TOPIC));
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
    Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
    while (iterator.hasNext()) {
      ConsumerRecord<String, String> record = iterator.next();
      System.err.println("Record:" + record);
      iterator.remove();
    }
  }
}
