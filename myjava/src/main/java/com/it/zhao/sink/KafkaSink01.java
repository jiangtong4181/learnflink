package com.it.zhao.sink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class KafkaSink01 {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
                "hadoop101:9092,hadoop102:9021,hadoop103:9092",
                "test01",
                new SimpleStringSchema()
        );
        ds1.addSink(kafkaProducer);
        env.execute("PrintSinkDemo01");
    }
}
