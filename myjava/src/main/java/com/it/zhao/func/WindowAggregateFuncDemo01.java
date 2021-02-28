package com.it.zhao.func;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;

//侧流输出提取迟到数据
public class WindowAggregateFuncDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<String> ds2 = ds1.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String s, long l) {
                        return Long.valueOf(s.split(",")[0]);
                    }
                }));
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds3 = ds2.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s.split(",")[1], Integer.parseInt(s.split(",")[2]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyBy = ds3.keyBy(t -> t.f0);
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyBy.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        window.aggregate(new MyAggFunc(), new MyProcessWindowFunc2()).print();
        env.execute();
    }


    public static class MyAggFunc implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {

        //初始化数据
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        //合并方法
        @Override
        public Integer add(Tuple2<String, Integer> value1, Integer value2) {
            return value1.f1 + value2;
        }

        //得到结果方法
        @Override
        public Integer getResult(Integer value) {
            return value;
        }

        //只有session方法才需要实现
        @Override
        public Integer merge(Integer integer, Integer acc1) {
            return null;
        }
    }

    public static class MyProcessWindowFunc2 extends ProcessWindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow> {

        private transient ValueState<Integer> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<Integer>("wc", Integer.class);
            state = getRuntimeContext().getState(stateDescriptor);

        }

        @Override
        public void process(String s, Context context, Iterable<Integer> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer value = state.value();
            if (value == null) {
                value = 0;
            }
            Integer next = elements.iterator().next();
            Integer total = value + next;
            state.update(total);
            out.collect(Tuple2.of(s, total));
        }
    }
}
