package com.it.zhao.keyedstate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedStateDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 4000));
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> ds2 = ds1.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String s) throws Exception {
                if (s.equals("error")) {
                    throw new RuntimeException("xxx");
                }
                String[] line = s.split(",");
                return Tuple3.of(line[0], line[1], Double.parseDouble(line[2]));
            }
        });
        KeyedStream<Tuple3<String, String, Double>, String> ds3 = ds2.keyBy(t -> t.f0);
        ds3.process(new KeyedProcessFunction<String, Tuple3<String, String, Double>, Tuple3<String, String, Double>>() {

            private transient MapState<String, Double> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态器
                MapStateDescriptor<String, Double> stateDescriptor = new MapStateDescriptor<>("wc", String.class, Double.class);
                mapState = getRuntimeContext().getMapState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple3<String, String, Double> value, Context context, Collector<Tuple3<String, String, Double>> collector) throws Exception {
                String province = value.f0;
                String city = value.f1;
                Double money = value.f2;
                Double cityMoney = mapState.get(city);
                if (cityMoney == null) {
                    cityMoney = 0.0;
                }
                Double totalMoney = money + cityMoney;
                mapState.put(city, totalMoney);
                value.f2 = totalMoney;
                collector.collect(value);
            }
        }).print();
        env.execute();
    }
}
