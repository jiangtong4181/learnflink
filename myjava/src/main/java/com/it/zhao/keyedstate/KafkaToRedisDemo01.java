package com.it.zhao.keyedstate;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class KafkaToRedisDemo01 {
    public static void main(String[] args) throws Exception {
        //System.setProperty("HADOOP_USER_NAME","root");

        //redis参数
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig
                .Builder()
                .setHost("hadoop101")
                .setDatabase(9)
                .setPassword("123456")
                .build();

        //flink提供的方法工具
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);
        //设置同步到fs中
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/flinkck"));
        //如果手动取消任务，不删除job的checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092");
        prop.setProperty("auto.offset.reset", "earliest");
        prop.setProperty("group.id", "g2");//通过外部传入
        //不自动提交偏移量
        prop.setProperty("enable.auto.commit", "false");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(
                "zhaochong01",//通过外部传入
                new SimpleStringSchema(),
                prop
        );
        //不将偏移量记录在kafka topic中
        kafkaSource.setCommitOffsetsOnCheckpoints(false);
        DataStreamSource<String> line = env.addSource(kafkaSource);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = line.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = flatMap.keyBy(t -> t.f0).sum(1);
        //写入到redis
        DataStreamSink<Tuple2<String, Integer>> sink = sum.addSink(new RedisSink<>(jedisPoolConfig, new MyRedisSink()));
        env.execute();
    }

    //redis sink
    private static class MyRedisSink implements RedisMapper<Tuple2<String,Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"WORD_COUNT");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> stringIntegerTuple2) {
            return stringIntegerTuple2.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> stringIntegerTuple2) {
            return stringIntegerTuple2.f1.toString();
        }
    }
}
