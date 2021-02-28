package com.it.zhao.window;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

public class EventTimeTumbingWindowKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","hadoop101:9092,hadoop102:9021,hadoop103:9092");
        prop.setProperty("auto.offset.reset","earliest");
        prop.setProperty("group.id","g1");
        prop.setProperty("enable.auto.commit","true");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(
                "wm01",
                new SimpleStringSchema(),
                prop
        );
        DataStreamSource<String> ds1 = env.addSource(kafkaSource);
        SingleOutputStreamOperator<String> watermarks = ds1.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String s) {
                return Long.parseLong(s.split(",")[0]);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds2 = watermarks.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s.split(",")[1], Integer.parseInt(s.split(",")[2]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> ds3 = ds2.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> ds4 = ds3.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        ds4.sum(1).print();
        env.execute("xxx");
    }
}
