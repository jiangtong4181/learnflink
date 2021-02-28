package com.it.zhao.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSource {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9021,hadoop103:9092");
        prop.setProperty("auto.offset.reset","earliest");
        prop.setProperty("group.id","g1");
        prop.setProperty("enable.auto.commit","true");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(
                "test01",
                new SimpleStringSchema(),
                prop
        );
        DataStreamSource<String> ds1 = env.addSource(kafkaSource);
        ds1.print();
        env.execute("KafkaSource");

    }
}
