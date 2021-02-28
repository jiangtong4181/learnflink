package com.it.zhao.keyedstate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyAtLeastOnceSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(30000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000));
        DataStreamSource<String> ds1 = env.addSource(new MySourceAtLeastOnce("C:\\Users\\Administrator\\Desktop\\ckfile"));
        DataStreamSource<String> ds2 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<String> ds3 = ds2.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                if (s.startsWith("error")) {
                    throw new RuntimeException("xx");
                }
                return s;
            }
        });
        DataStream<String> union = ds3.union(ds1);
        union.print();
        env.execute();
    }
}
