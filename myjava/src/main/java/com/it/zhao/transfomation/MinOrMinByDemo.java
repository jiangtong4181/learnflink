package com.it.zhao.transfomation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MinOrMinByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> ds2 = ds.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String s) throws Exception {
                String[] words = s.split(",");
                return Tuple3.of(words[0], words[1], Integer.parseInt(words[2]));
            }
        });

        KeyedStream<Tuple3<String, String, Integer>, String> ds3 = ds2.keyBy(t -> t.f0);
        //第二个参数是判断，如果相等的条件下，其他没参加分组的字段要不要用最新值填充，false 是填充，true是不填充
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> ds4 = ds3.minBy(2,false);
        //SingleOutputStreamOperator<Tuple3<String, String, Integer>> ds4 = ds3.min(2);
        ds4.print();
        env.execute();
    }
}
