package com.it.zhao.keyedstate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutPutDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        //定义侧流tag
        OutputTag<String> oddTag = new OutputTag<String>("odd") {
        };
        OutputTag<String> evenTag = new OutputTag<String>("even") {
        };
        OutputTag<String> otherTag = new OutputTag<String>("other") {
        };
        SingleOutputStreamOperator<String> process = ds1.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {

                try {
                    int i = Integer.parseInt(s);
                    if (i % 2 == 0) {
                        //对侧流打标签
                        context.output(evenTag, s);
                    }
                    if (i % 2 != 0) {
                        context.output(oddTag, s);
                    }
                } catch (Exception e) {
                    context.output(otherTag, s);
                }
                collector.collect(s);
            }
        });
        //取出侧流
        process.getSideOutput(oddTag).print("odd   : ");
        process.getSideOutput(evenTag).print("even  : ");
        process.getSideOutput(otherTag).print("other : ");
        process.print("all   : ");
        env.execute();
    }
}
