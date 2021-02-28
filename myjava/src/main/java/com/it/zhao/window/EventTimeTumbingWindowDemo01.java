package com.it.zhao.window;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//watermark 上游并行度为2，因此需要每个分区内的时间都达到（最大eventtime - 乱序延迟时间 >= 窗口结束时间）才会生成窗口
public class EventTimeTumbingWindowDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> ds2 = ds1.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String s) throws Exception {
                return Tuple3.of(Long.parseLong(s.split(",")[0]), s.split(",")[1], Integer.parseInt(s.split(",")[2]));
            }
        }).setParallelism(2);
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> watermarks = ds2.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> v3) {
                return v3.f0;
            }
        });
        SingleOutputStreamOperator<Tuple> ds3 = watermarks.project(1, 2);
        KeyedStream<Tuple, Tuple> ds4 = ds3.keyBy(0);
        WindowedStream<Tuple, Tuple, TimeWindow> ds5 = ds4.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        ds5.sum(1).print();
        env.execute("xxx");
    }
}
