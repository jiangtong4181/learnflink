package com.it.zhao.source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.util.UUID;


public class CustomSource02 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(3);
        DataStreamSource ds1 = env.addSource(new MySource02());
        ds1.print();
        env.execute();
    }


    public static class MySource02 extends RichParallelSourceFunction<String>{
        private boolean flag = true;
        //调用构造方法
        //调用open方法，只会调用一次
        //调用run方法
        //调用cancle方法停止run方法
        //调用close方法释放资源
        @Override
        public void open(Configuration parameters) throws Exception {
            //获得具体的sutask
            int index = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(index+"  open");
        }

        @Override
        public void close() throws Exception {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(index+"  close");
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(index);
            while(flag){
                sourceContext.collect(UUID.randomUUID().toString());
                Thread.sleep(3000);
            }
        }

        @Override
        public void cancel() {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println(index+"  cancel");
            flag = false;
        }
    }
}
