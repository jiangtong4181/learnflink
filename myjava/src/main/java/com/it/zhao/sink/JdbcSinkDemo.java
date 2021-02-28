package com.it.zhao.sink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class JdbcSinkDemo {
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(5000);
        //从socket中读取数据
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds2 = ds1.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }

            }
        });
        KeyedStream<Tuple2<String, Integer>, String> ds3 = ds2.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds4 = ds3.sum(1);

        //新版本mysql驱动：com.mysql.cj.jdbc.Driver
        //ON DUPLICATE KEY UPDATE 如果存在就更新
        /*
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-jdbc_2.11</artifactId>
            <version>1.12.0</version>
        </dependency>
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>8.0.11</version>
        </dependency>
         */
        ds4.addSink(
                JdbcSink.sink(
                    "insert into flink_learn_sink (word,count) values (?,?) ON DUPLICATE KEY UPDATE count = ?",
                    (ps,t)->{
                        ps.setString(1,t.f0);
                        ps.setInt(2,t.f1);
                        ps.setInt(3,t.f1);
                    },
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl("jdbc:mysql://localhost:3306/learn?useSSL=false&useUnicode=true&characterEncoding=utf-8")
                            .withDriverName("com.mysql.cj.jdbc.Driver").withUsername("root").withPassword("1234").build()
                    )
                );

        env.execute("MyRedisSink");
    }
}

