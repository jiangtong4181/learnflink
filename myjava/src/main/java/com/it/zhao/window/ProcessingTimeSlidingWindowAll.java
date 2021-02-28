package com.it.zhao.window;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class ProcessingTimeSlidingWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Integer> ds2 = ds1.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s);
            }
        });
        //窗口长度是12秒，滑动距离是3秒
        AllWindowedStream<Integer, TimeWindow> ds3 = ds2.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(12), Time.seconds(3)));
        ds3.sum(0).print();
        env.execute("xxx");
    }
}