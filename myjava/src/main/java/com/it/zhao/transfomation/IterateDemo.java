package com.it.zhao.transfomation;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//迭代计算使用
public class IterateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Long> ds2 = ds1.map(t -> Long.parseLong(t));
        //将ds2迭代
        IterativeStream<Long> ds2it = ds2.iterate();
        //对迭代集合进行map操作
        SingleOutputStreamOperator<Long> itBody = ds2it.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                return aLong = aLong - 2;
            }
        });
        SingleOutputStreamOperator<Long> feedBack = itBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong > -10;
            }
        });

        //迭代计算以什么方式（feedBack）结尾
        ds2it.closeWith(feedBack);
//        SingleOutputStreamOperator<Long> map = itBody.map(new MapFunction<Long, Long>() {
//            @Override
//            public Long map(Long aLong) throws Exception {
//                return aLong;
//            }
//        });
        //取出所有经过处理符合条件的数据
        SingleOutputStreamOperator<Long> out = itBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong <= 0;
            }
        });
        out.print();
        env.execute();
    }
}
