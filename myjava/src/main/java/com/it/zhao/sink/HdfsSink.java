package com.it.zhao.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class HdfsSink {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(5000);
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);

        //构建文件滚动生成策略
        DefaultRollingPolicy<String, String> build = DefaultRollingPolicy.builder()
                .withRolloverInterval(30 * 1000L)
                .withMaxPartSize(1024L * 1024l * 100L).build();

        //创建streamingfilesink，数据以行格式写入
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(
                new Path("hdfs://hadoop101:8020/flinkHdfs02"),
                new SimpleStringEncoder<String>("UTF-8")
        ).withRollingPolicy(build).build();

        ds1.addSink(sink);
        env.execute("HdfsSink");
    }
}
