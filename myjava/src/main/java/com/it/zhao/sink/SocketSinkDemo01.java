package com.it.zhao.sink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketSinkDemo01 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        ds1.writeToSocket("hadoop102",9999,new SimpleStringSchema());
        env.execute("PrintSinkDemo01");
    }
}
