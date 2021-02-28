package com.it.zhao.transfomation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//带有bean结构的keyby
//一定要有无参的构造方法！！！
public class KeyByDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("hadoop101", 8888);
        SingleOutputStreamOperator<DataBean> ds2 = ds1.map(new MapFunction<String, DataBean>() {
            @Override
            public DataBean map(String s) throws Exception {
                String[] words = s.split(",");
                return new DataBean(words[0], words[1], Double.parseDouble(words[2]));
            }
        });
        KeyedStream<DataBean, Tuple> ds3 = ds2.keyBy("province", "city");
        SingleOutputStreamOperator<DataBean> ds4 = ds3.sum("money");
        ds4.print();
        env.execute();
    }

    public static class DataBean{

        public DataBean() {
        }

        public String province;
        public String city;
        public double money;

        public DataBean(String province, String city, double money) {
            this.province = province;
            this.city = city;
            this.money = money;
        }

        @Override
        public String toString() {
            return "DataBean{" +
                    "province='" + province + '\'' +
                    ", city='" + city + '\'' +
                    ", money=" + money +
                    '}';
        }
    }
}
