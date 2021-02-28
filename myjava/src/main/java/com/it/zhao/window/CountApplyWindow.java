package com.it.zhao.window;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Comparator;


public class CountApplyWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Integer> ds2 = ds1.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s);
            }
        });
        AllWindowedStream<Integer, GlobalWindow> windowCount = ds2.countWindowAll(5);
        /*
        本方法为sum
        iterable 输入的数据
        collector 输出的数据
         */
//        SingleOutputStreamOperator<Integer> sum = windowCount.apply(new AllWindowFunction<Integer, Integer, GlobalWindow>() {
//            @Override
//            public void apply(GlobalWindow globalWindow, Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
//                Integer sum = 0;
//                for (Integer integer : iterable) {
//                    sum += integer;
//                }
//                collector.collect(sum);
//            }
//        });
        //本方法为排序输出
        SingleOutputStreamOperator<Integer> sum = windowCount.apply(new AllWindowFunction<Integer, Integer, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow globalWindow, Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
                ArrayList<Integer> list = new ArrayList<>();
                for (Integer integer : iterable) {
                    list.add(integer);
                }
                list.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o1 - o2;
                    }
                });
                for(int i=0;i<=2;i++){
                    collector.collect(list.get(i));
                }
            }
        });
        sum.print().setParallelism(1);
        env.execute();
    }
}
