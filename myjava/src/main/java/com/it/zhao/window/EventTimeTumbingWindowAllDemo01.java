package com.it.zhao.window;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class EventTimeTumbingWindowAllDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);

        /*
        1609430400000,1
        1609430403000,1
        1609430404998,1
        1609430405000,1
        1609430409998,1
        1609430412000,1
        [1609430400000,1609430405000) 严格地说 [1609430400000,1609430404999]
        公式：当前分区中携带数据中的最大eventtime - 乱序延迟时间 >= 窗口结束时间 就会产生新窗口！！
         */
        SingleOutputStreamOperator<String> watermarks = ds1.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String s) {
                return Long.parseLong(s.split(",")[0]);
            }
        });
        SingleOutputStreamOperator<Integer> ds2 = watermarks.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s.split(",")[1]);
            }
        });
        AllWindowedStream<Integer, TimeWindow> ds3 = ds2.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));
        ds3.sum(0).print();
        env.execute("xxx");
    }
}
