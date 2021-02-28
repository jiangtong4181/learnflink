package com.it.zhao.func;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
//通过定时器，实现类似滚动窗口
public class OnTimerFunc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds2 = ds1.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] words = s.split(",");
                return Tuple2.of(words[0], Integer.parseInt(words[1]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> ds3 = ds2.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds4 = ds3.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private transient ValueState<Integer> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("wc", Integer.class);
                state = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                long processingTime = context.timerService().currentProcessingTime();
                //每一分钟触发一次
                long timer = processingTime - processingTime % 60000 + 60000;
                //如果注册了相同时间的timer，后面的会将前面的覆盖
                context.timerService().registerProcessingTimeTimer(timer);
                Integer count = value.f1;
                Integer historyCount = state.value();
                if (historyCount == null) {
                    historyCount = 0;
                }
                Integer totalCount = count + historyCount;
                state.update(totalCount);
            }

            //定时器触发，输出当前的结果
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(ctx.getCurrentKey(), state.value()));
            }
        });
        ds4.print();
        env.execute();
    }
}
