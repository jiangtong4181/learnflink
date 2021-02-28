package com.it.zhao.keyedstate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.HashSet;
//统计次数和人数demo
public class DistinctPeople {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
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
        //计算人数和次数，人数需要去重，次数不用去重
        SingleOutputStreamOperator<Tuple4<String, String, Integer, Integer>> streamOperator = keyedStream.map(new RichMapFunction<Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>>() {
            private transient ValueState<Integer> nondisState;
            private transient ValueState<HashSet<String>> hashSetValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> nondisStateDescripter = new ValueStateDescriptor<>("wc-nondis", Integer.class);
                nondisState = getRuntimeContext().getState(nondisStateDescripter);
                ValueStateDescriptor<HashSet<String>> hashSetValueStateDescriptor = new ValueStateDescriptor<>("wc-dis", TypeInformation.of(new TypeHint<HashSet<String>>() {}));
                hashSetValueState = getRuntimeContext().getState(hashSetValueStateDescriptor);
            }

            @Override
            public Tuple4<String, String, Integer, Integer> map(Tuple3<String, String, String> value) throws Exception {
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
                return Tuple4.of(value.f1, value.f2, totalnoDis, hashSet.size());
            }
        });
        streamOperator.print();
        env.execute();
    }
}
