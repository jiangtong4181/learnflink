package com.it.zhao.transfomation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KeyByDemo01 {
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
        //可以使用f0，f1，是因为tuple里面的字段名称是f0,f1
        KeyedStream<Tuple3<String, String, Integer>, Tuple> ds3 = ds2.keyBy(0, 1);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> ds4 = ds3.sum(2);
        ds4.print();
        env.execute();
    }
}
