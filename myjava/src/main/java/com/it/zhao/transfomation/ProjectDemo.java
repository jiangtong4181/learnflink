package com.it.zhao.transfomation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//就是投影，只能针对tuple类型的数据，可以变动元素位置，或者只挑选其中的某个元素
public class ProjectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, String>> ds2 = ds1.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String s) throws Exception {
                String[] words = s.split(" ");
                return Tuple3.of(words[0], words[1], words[2]);
            }
        });
        SingleOutputStreamOperator<Tuple> ds3 = ds2.project(2, 1, 0);
        ds3.print();
        env.execute();
    }
}
