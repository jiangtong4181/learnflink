package com.it.zhao.window;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

//会话窗口是指，距离上一次输入的最后一条数据的时间间隔大于指定的时间，就会触发一个窗口进行计算
//带keyed的，是按照分区内分组内的数据进行触发窗口
public class ProcessingTimeSessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds2 = ds1.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s.split(",")[0], Integer.parseInt(s.split(",")[1]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> ds3 = ds2.keyBy(t -> t.f0);
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> ds4 = ds3.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
        ds4.sum(1).print();
        env.execute("xxx");
    }
}
