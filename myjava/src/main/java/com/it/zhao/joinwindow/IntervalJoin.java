package com.it.zhao.joinwindow;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        DataStreamSource<String> ds2 = env.socketTextStream("hadoop101", 9999);
        SingleOutputStreamOperator<String> watermarks1 = ds1.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String s) {
                return Long.parseLong(s.split(",")[0]);
            }
        });
        SingleOutputStreamOperator<String> watermarks2 = ds2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String s) {
                return Long.parseLong(s.split(",")[0]);
            }
        });

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> map1 = watermarks1.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String s) throws Exception {
                return Tuple3.of(Long.parseLong(s.split(",")[0]), s.split(",")[1], Integer.parseInt(s.split(",")[2]));
            }
        });

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> map2 = watermarks2.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String s) throws Exception {
                return Tuple3.of(Long.parseLong(s.split(",")[0]), s.split(",")[1], Integer.parseInt(s.split(",")[2]));
            }
        });
        SingleOutputStreamOperator<Tuple6<Long, String, Integer, Long, String, Integer>> process = map1.keyBy(t -> t.f1)
                .intervalJoin(map2.keyBy(t -> t.f1))
                .between(Time.seconds(-1), Time.seconds(1))//如图所示时间区间
                .upperBoundExclusive()//前闭后开区间[1,2)
                .process(new ProcessJoinFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>, Tuple6<Long, String, Integer, Long, String, Integer>>() {
                    @Override
                    public void processElement(Tuple3<Long, String, Integer> v1, Tuple3<Long, String, Integer> v2, Context context, Collector<Tuple6<Long, String, Integer, Long, String, Integer>> collector) throws Exception {
                        collector.collect(Tuple6.of(v1.f0, v1.f1, v1.f2, v2.f0, v2.f1, v2.f2));
                    }
                });
        process.print();
        env.execute();
    }
}
