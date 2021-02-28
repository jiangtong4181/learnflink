package com.it.zhao.transfomation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Integer> ds2 = env.socketTextStream("hadoop101", 9999).map(t -> Integer.parseInt(t));
        ConnectedStreams<String, Integer> ds3 = ds1.connect(ds2);
        //connect 共享状态
        SingleOutputStreamOperator<String> ds4 = ds3.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String s) throws Exception {
                return s.toUpperCase();
            }

            @Override
            public String map2(Integer integer) throws Exception {
                return (integer * 2) + "";
            }
        });
        ds4.print();
        env.execute();
    }
}
