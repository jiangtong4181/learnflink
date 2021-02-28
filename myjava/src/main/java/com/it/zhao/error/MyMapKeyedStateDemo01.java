package com.it.zhao.error;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.util.HashMap;

public class MyMapKeyedStateDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //env.enableCheckpointing(5000);
        //自己定义的hashmap如果程序出错不能容错，中间状态不会保存
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,4000));
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds2 = ds1.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    if ("error".equals(word)) {
                        throw new RuntimeException("异常");
                    }
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> ds3 = ds2.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds4 = ds3.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private HashMap<String, Integer> counter = new HashMap<>();

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                String word = value.f0;
                Integer currCount = value.f1;
                Integer historyCount = counter.get(word);
                if (historyCount == null) {
                    historyCount = 0;
                }
                int totalCount = currCount + historyCount;
                counter.put(word, totalCount);
                return Tuple2.of(word, totalCount);
            }
        });
        ds4.print();
        env.execute();
    }
}
