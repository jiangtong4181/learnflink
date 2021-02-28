package com.it.zhao.transfomation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class FlatMapDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);

        SingleOutputStreamOperator<String> ds2 = ds1.transform("myflatmap", TypeInformation.of(String.class), new MyFlatMap());
        ds2.print();

        env.execute("FlatMapDemo02");
    }

    public static class MyFlatMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String,String>{

        @Override
        public void processElement(StreamRecord<String> streamRecord) throws Exception {
            String[] words = streamRecord.getValue().split(" ");
            for (String word : words) {
                if(!"error".equals(word)){
                    output.collect(streamRecord.replace(word));
                }
            }
        }
    }
}
