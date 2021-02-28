package com.it.zhao.keyedstate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.util.HashSet;

public class BroadCastStateDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> dss = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, String>> dss2 = dss.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String s) throws Exception {
                String[] words = s.split(",");
                return Tuple3.of(words[0], words[1], words[2]);
            }
        });

        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>("wc", String.class, String.class);

        BroadcastStream<Tuple3<String, String, String>> broadcast = dss2.broadcast(descriptor);


        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 9999);
        SingleOutputStreamOperator<Tuple3<String, String, String>> ds2 = ds1.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String s) throws Exception {
                String[] words = s.split(" ");
                return Tuple3.of(words[0], words[1], words[2]);
            }
        });
        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyedStream = ds2.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, String> value) throws Exception {
                return Tuple2.of(value.f1, value.f2);
            }
        });

        /*
        第一次是keby的key，第二个参数是keyby的流，第三个是广播的流，第四个是输出
         */
        keyedStream.connect(broadcast).process(new KeyedBroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>>() {
            private transient ValueState<Integer> nondisState;
            private transient ValueState<HashSet<String>> hashSetValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> nondisStateDescripter = new ValueStateDescriptor<>("wc-nondis", Integer.class);
                nondisState = getRuntimeContext().getState(nondisStateDescripter);
                ValueStateDescriptor<HashSet<String>> hashSetValueStateDescriptor = new ValueStateDescriptor<>("wc-dis", TypeInformation.of(new TypeHint<HashSet<String>>() {
                }));
                hashSetValueState = getRuntimeContext().getState(hashSetValueStateDescriptor);
            }

            /**
             * 处理活动的每一条数据
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement(Tuple3<String, String, String> value, ReadOnlyContext ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
                //拿到维度数据从广播变量里面
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);
                //处理次数的逻辑
                Integer nodisCount = nondisState.value();
                if (nodisCount == null) {
                    nodisCount = 0;
                }
                Integer totalnoDis = nodisCount + 1;
                nondisState.update(totalnoDis);
                //处理去重后人数的逻辑
                HashSet<String> hashSet = hashSetValueState.value();
                if (hashSet == null) {
                    hashSet = new HashSet<String>();
                }
                //通过hashset去重
                hashSet.add(value.f0);
                hashSetValueState.update(hashSet);
                //关联拿到数据
                String eventId = value.f2;
                String eventName = broadcastState.get(eventId);
                out.collect(Tuple4.of(value.f1, eventName, totalnoDis, hashSet.size()));
            }

            /**
             * 处理输入的每一条维度数据
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
                //insert,update,delete
                String type = value.f0;
                //eventid
                String eventId = value.f1;
                //eventname
                String eventName = value.f2;
                //获取广播状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(descriptor);
                if (type.equals("DELETE")) {
                    broadcastState.remove(eventId);
                } else {
                    broadcastState.put(eventId, eventName);
                }
            }
        }).print();
        env.execute();
    }
}
