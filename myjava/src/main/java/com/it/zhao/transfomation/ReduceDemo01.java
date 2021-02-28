package com.it.zhao.transfomation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class ReduceDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> ds2 = ds1.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String s) throws Exception {
                String[] words = s.split(",");
                return Tuple3.of(words[0], words[1], Integer.parseInt(words[2]));
            }
        });

//        KeyedStream<Tuple3<String, String, Integer>, String> ds3 = ds2.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
//            @Override
//            public String getKey(Tuple3<String, String, Integer> stringStringIntegerTuple3) throws Exception {
//                return stringStringIntegerTuple3.f0 + stringStringIntegerTuple3.f1;
//            }
//        });
        /*
        1，可以使用字符串拼接的方式
        2，也可以使用tuple的方式作为分组的key，因为
        3，都重写了tostring方法，hashcode方法
        也可以使用bean方式分组
         */
        KeyedStream<Tuple3<String, String, Integer>, Tuple2<String, String>> ds3 = ds2.keyBy(new KeySelector<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, Integer> stringStringIntegerTuple3) throws Exception {
                return Tuple2.of(stringStringIntegerTuple3.f0, stringStringIntegerTuple3.f1);
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> ds4 = ds3.reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> t1, Tuple3<String, String, Integer> t2) throws Exception {
                t1.f2 = t1.f2 + t2.f2;
                return t1;
            }
        });
        ds4.print();
        env.execute();
    }
}
