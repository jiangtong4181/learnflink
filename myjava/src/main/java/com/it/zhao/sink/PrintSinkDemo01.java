package com.it.zhao.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class PrintSinkDemo01 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        ds1.addSink(new MySink01()).name("MySink01");
        env.execute("PrintSinkDemo01");
    }

    public static class MySink01 extends RichSinkFunction<String> {
        private int index;

        @Override
        public void open(Configuration parameters) throws Exception {
            index = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void invoke(String value, Context context) {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(index + 1 + "> "+value);
        }
    }
}
