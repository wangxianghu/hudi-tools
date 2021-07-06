package com.xianghu.hudi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerClient {
  public static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerClient.class);
  public static void main(String[] args) {
    //1.加载配置信息
    Properties prop = loadProperties();

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

    // 订阅消息
    String topic = "hudi_test_topic";
    consumer.subscribe(Collections.singletonList(topic));

    // 读取、解析消息
    ConsumerRecords<String, String> records = consumer.poll(1000);

    while (true) {
      if (!records.isEmpty()) {
        for (ConsumerRecord<String, String> record : records) {
          System.out.println(record.value());
          LOG.info("receive msg : {}", record.value());
        }
      }
    }

  }

  private static Properties loadProperties() {
    Properties prop = new Properties();
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, "hello");
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    return prop;
  }
}
