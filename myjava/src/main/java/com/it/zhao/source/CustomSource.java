package com.it.zhao.source;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Arrays;
import java.util.List;

public class CustomSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource ds1 = env.addSource(new MySource01());
        ds1.print();
        env.execute();

    }


    public static class MySource01 implements SourceFunction<Integer> {

        @Override
        public void run(SourceContext ctx) throws Exception {
            List<Integer> lists = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
            for (Integer list : lists) {
                ctx.collect(list);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
