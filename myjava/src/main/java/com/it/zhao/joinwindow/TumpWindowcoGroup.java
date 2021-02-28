package com.it.zhao.joinwindow;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class TumpWindowcoGroup {
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
        map1.coGroup(map2)
                .where(t -> t.f1)
                .equalTo(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>, Tuple5<Long, String, Integer, Long, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<Long, String, Integer>> iterable1, Iterable<Tuple3<Long, String, Integer>> iterable2, Collector<Tuple5<Long, String, Integer, Long, Integer>> collector) throws Exception {
                        for (Tuple3<Long, String, Integer> v1 : iterable1) {
                            boolean join = false;
                            for (Tuple3<Long, String, Integer> v2 : iterable2) {
                                join = true;
                                collector.collect(Tuple5.of(v1.f0, v1.f1, v1.f2, v2.f0, v2.f2));
                            }
                            if (join == false) {
                                collector.collect(Tuple5.of(v1.f0, v1.f1, v1.f2, null, null));
                            }
                        }
                    }
                }).print();
        env.execute();
    }
}
