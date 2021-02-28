package com.it.zhao.finalitem;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkUtils {

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<T> createKafkaStream(ParameterTool parameterTool, Class<? extends DeserializationSchema<T>> clazz,String topic) throws Exception{
        String checkpointPath = parameterTool.getRequired("checkpoint.path");
        Long checkpointInterval = parameterTool.getLong("checkpoint.interval");
        String bootstrapServers = parameterTool.get("bootstrap.servers");
        String autoOffsetReset = parameterTool.get("auto.offset.reset");
        String groupId = parameterTool.get("group.id");
        String enableAutoCommit = parameterTool.get("enable.auto.commit");
        boolean setCommitOffsetsOnCheckpoints = parameterTool.getBoolean("setCommitOffsetsOnCheckpoints");
        //List<String> topicList = Arrays.asList(parameterTool.get("kafka.topics").split(","));

        env.enableCheckpointing(checkpointInterval);
        env.setStateBackend(new FsStateBackend(checkpointPath));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", bootstrapServers);
        prop.setProperty("auto.offset.reset", autoOffsetReset);
        prop.setProperty("group.id", groupId);
        prop.setProperty("enable.auto.commit", enableAutoCommit);

        FlinkKafkaConsumer<T> kafkaSource = new FlinkKafkaConsumer<>(
                topic,
                clazz.newInstance(),
                prop
        );

        kafkaSource.setCommitOffsetsOnCheckpoints(setCommitOffsetsOnCheckpoints);
        return env.addSource(kafkaSource);

    }

    public static <T> DataStream<T> createKafkaStream(ParameterTool parameterTool, Class<? extends DeserializationSchema<T>> clazz) throws Exception {
        String checkpointPath = parameterTool.getRequired("checkpoint.path");
        Long checkpointInterval = parameterTool.getLong("checkpoint.interval");
        String bootstrapServers = parameterTool.get("bootstrap.servers");
        String autoOffsetReset = parameterTool.get("auto.offset.reset");
        String groupId = parameterTool.get("group.id");
        String enableAutoCommit = parameterTool.get("enable.auto.commit");
        boolean setCommitOffsetsOnCheckpoints = parameterTool.getBoolean("setCommitOffsetsOnCheckpoints");
        List<String> topicList = Arrays.asList(parameterTool.get("kafka.topics").split(","));

        env.enableCheckpointing(checkpointInterval);
        env.setStateBackend(new FsStateBackend(checkpointPath));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", bootstrapServers);
        prop.setProperty("auto.offset.reset", autoOffsetReset);
        prop.setProperty("group.id", groupId);
        prop.setProperty("enable.auto.commit", enableAutoCommit);

        FlinkKafkaConsumer<T> kafkaSource = new FlinkKafkaConsumer<>(
                topicList,
                clazz.newInstance(),
                prop
        );

        kafkaSource.setCommitOffsetsOnCheckpoints(setCommitOffsetsOnCheckpoints);
        return env.addSource(kafkaSource);
    }
}
