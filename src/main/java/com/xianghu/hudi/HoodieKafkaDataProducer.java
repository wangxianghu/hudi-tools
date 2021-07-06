package com.xianghu.hudi;

import com.xianghu.hudi.data.generator.HoodieDataCreater;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class HoodieKafkaDataProducer {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    String topic = "hudi_test_topic";
    int batch = 20;

    Producer<String, String> producer = new KafkaProducer<>(props);

    for (String data : HoodieDataCreater.create(batch)) {
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, data);
      producer.send(record);
      System.out.println("消息发送成功:" + data);
    }
    producer.close();
  }
}
