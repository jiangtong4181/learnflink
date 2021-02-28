package com.it.zhao.source;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.util.Arrays;

public class StreamWordCountLambda {
    public static void main(String[] args) throws Exception{
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(8);
        DataStreamSource<String> ds = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<String> ds2 = ds.flatMap((String line,Collector<String> out) ->
                Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds3 = ds2.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING,Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds4 = ds3.keyBy(0).sum(1);
        ds4.print();

        env.execute("StreamWordCountLambda");
    }
}
