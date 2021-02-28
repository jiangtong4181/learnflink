package com.it.zhao.keyedstate;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.Charset;

public class KafkaStringSerializationShema implements KafkaSerializationSchema<String> {
    private String topic;
    private String charset;

    public KafkaStringSerializationShema() {
    }

    public KafkaStringSerializationShema(String topic, String charset) {
        this.topic = topic;
        this.charset = charset;
    }

    public KafkaStringSerializationShema(String topic) {
        this(topic, "UTF-8");
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
        byte[] bytes = s.getBytes(Charset.forName(charset));
        return new ProducerRecord<>(topic, bytes);

    }
}
