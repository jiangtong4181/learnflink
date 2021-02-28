package com.it.zhao.keyedstate;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.util.Properties;

public class KafkaToKafkaDemo01 {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/flink20200221"));

        //kafka参数
        Properties prop = new Properties();
        //prop.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "");
        prop.setProperty("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092");
        prop.setProperty("auto.offset.reset", "earliest");
        prop.setProperty("group.id", "g1");//通过外部传入
        //不自动提交偏移量
        prop.setProperty("enable.auto.commit", "false");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(
                "wm01",//通过外部传入
                new SimpleStringSchema(),
                prop
        );
        //不将偏移量记录在kafka topic中
        kafkaSource.setCommitOffsetsOnCheckpoints(false);

        DataStreamSource<String> line = env.addSource(kafkaSource);
        SingleOutputStreamOperator<String> filter = line.filter(t -> !t.startsWith("error"));

        //老版本方法，使用的是默认的语义，不是只消费一次，而是至少消费一次
//        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
//                "hadoop101:9092,hadoop102:9092,hadoop103:9092",
//                "wm18",
//                new SimpleStringSchema()
//        );
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "wm18",
                new KafkaStringSerializationShema("wm18"),
                prop,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        filter.addSink(kafkaProducer);
        env.execute();
    }
}
