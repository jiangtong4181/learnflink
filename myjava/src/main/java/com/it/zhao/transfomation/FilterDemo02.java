package com.it.zhao.transfomation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class FilterDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4, 5, 6, 7);

        SingleOutputStreamOperator<Integer> ds2 = ds1.transform("filter", TypeInformation.of(Integer.class), new MyFilter());
        ds2.print();
        env.execute("FilterDemo02");
    }

    public static class MyFilter extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer,Integer> {

        @Override
        public void processElement(StreamRecord<Integer> streamRecord) throws Exception {
            if(streamRecord.getValue() % 2 == 0){
                output.collect(streamRecord);
            }
        }
    }
}
