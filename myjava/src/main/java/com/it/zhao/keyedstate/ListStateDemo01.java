package com.it.zhao.keyedstate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.List;

public class ListStateDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 4000));
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Tuple2<String, String>> ds2 = ds1.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                if (s.equals("error")) {
                    throw new RuntimeException("xxx");
                }
                String[] line = s.split(",");
                return Tuple2.of(line[0], line[1]);
            }
        });
        KeyedStream<Tuple2<String, String>, String> ds3 = ds2.keyBy(t -> t.f0);
        ds3.process(new KeyedProcessFunction<String, Tuple2<String,String>, Tuple2<String,List<String>>>() {
            private transient ListState<String> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<>("wc", String.class);
                listState = getRuntimeContext().getListState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, String> value, Context context, Collector<Tuple2<String, List<String>>> collector) throws Exception {
                String action = value.f1;
                listState.add(action);
                ArrayList<String> list = new ArrayList<>();
                for (String s : listState.get()) {
                    list.add(s);
                }
                listState.update(list);
                collector.collect(Tuple2.of(value.f0,list));
            }
        }).print();
        env.execute();
    }
}
