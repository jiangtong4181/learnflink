package com.it.zhao.transfomation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamMap;

public class MapDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4, 5, 6, 7);
        //map底层算子，TypeInformation.of(int.class) 返回值类型，new StreamMap<>处理方法
        SingleOutputStreamOperator<Integer> ds2 = ds1.transform("mymap", TypeInformation.of(int.class), new StreamMap<>(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        }));
        ds2.print();
        env.execute("MapDemo01");
    }
}
