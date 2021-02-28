package com.it.zhao.window;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class KafkaProducer01 {
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9021,hadoop103:9092");
        prop.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);
        String topic = "wm01";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, 1,null, "5000,flink,3");
        kafkaProducer.send(record);
        System.out.println("发送成功!");
        kafkaProducer.close();
    }
}
