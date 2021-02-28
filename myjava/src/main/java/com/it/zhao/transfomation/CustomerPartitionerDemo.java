package com.it.zhao.transfomation;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CustomerPartitionerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Tuple2<String,Integer>> ds2 = ds1.map(new RichMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> map(String s) throws Exception {
                int index = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(s,index);
            }
        });


        DataStream<Tuple2<String, Integer>> ds3 = ds2.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String s, int i) {
                int res = 0;
                if ("spark".equals(s)) {
                    res = 1;
                } else if ("flink".equals(s)) {
                    res = 2;
                } else if ("hadoop".equals(s)) {
                    res = 3;
                } else if ("hue".equals(s)) {
                    res = 4;
                } else if ("hive".equals(s)) {
                    res = 5;
                }
                return res;

            }
        }, tp -> tp.f0);
        ds3.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                int index = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println(value.f0 + ",上游 " +value.f1 + "  ->  下游 " + index);
            }
        });
        env.execute();
    }
}
