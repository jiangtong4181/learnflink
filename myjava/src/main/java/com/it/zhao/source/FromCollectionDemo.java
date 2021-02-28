package com.it.zhao.source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;

public class FromCollectionDemo {
    public static void main(String[] args) throws Exception {
        //创建conf
        Configuration conf = new Configuration();
        //设置端口
        conf.setInteger("rest.port",8081);
        //创建本地webui，但是需要导入包
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //默认并行度为1，是有限的source数据流，程序执行完就退出了，做实验用
        DataStreamSource<Integer> ds1 = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6));
        ds1.print();
        env.execute("FromCollectionDemo");
    }
}
