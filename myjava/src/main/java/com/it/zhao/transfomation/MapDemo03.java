package com.it.zhao.transfomation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class MapDemo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4, 5, 6, 7);

        SingleOutputStreamOperator<Integer> ds2 = ds1.transform("mymap", TypeInformation.of(int.class), new MyMap());
        ds2.print();
        env.execute("MapDemo01");
    }

    public static class MyMap extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer,Integer> {

        @Override
        public void processElement(StreamRecord<Integer> streamRecord) throws Exception {
            output.collect(streamRecord.replace(streamRecord.getValue()*2));
        }
    }
}
