package com.it.zhao.finalitem;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class AdvActivityCount {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        DataStream<String> line = FlinkUtils.createKafkaStream(parameterTool, SimpleStringSchema.class);
        SingleOutputStreamOperator<ActivityBean> ds1 = line.process(new ProcessFunction<String, ActivityBean>() {
            @Override
            public void processElement(String value, Context ctx, Collector<ActivityBean> out) throws Exception {
                String[] words = value.split(",");
                String time = words[0];
                String uid = words[1];
                String aid = words[2];
                String eid = words[3];
                out.collect(ActivityBean.of(uid, aid, time, eid));
            }
        });

        KeyedStream<ActivityBean, Tuple2<String, String>> keyedStream = ds1.keyBy(new KeySelector<ActivityBean, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(ActivityBean activityBean) throws Exception {
                return Tuple2.of(activityBean.aid, activityBean.eid);
            }
        });

        keyedStream.process(new MyActivityCountFunc()).print();
        FlinkUtils.env.execute();
    }

    private static class MyActivityCountFunc extends KeyedProcessFunction<Tuple2<String, String>, ActivityBean, ActivityBean> {

        //按小时未去重次数
        private transient MapState<String, Long> hourMapState;
        //按小时去重次数
        private transient MapState<String, Long> disHourMapState;
        //判断按小时去重的布隆过滤器
        private transient MapState<String, BloomFilter<String>> bloomFilterMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> hourStateDescripator = new MapStateDescriptor<>("Wccount", String.class, Long.class);
            MapStateDescriptor<String, Long> disHourStateDescripator = new MapStateDescriptor<>("disWccount", String.class, Long.class);
            //设置ttl,定期清除布隆过滤器
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(90))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//更新还会重新计时
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            //布隆过滤器
            MapStateDescriptor<String, BloomFilter<String>> hourStateDescripatorBloom = new MapStateDescriptor<>("disWccount-bloom",
                    TypeInformation.of(String.class),
                    TypeInformation.of(new TypeHint<BloomFilter<String>>() {
                    }));
            //绑定ttl
            hourStateDescripatorBloom.enableTimeToLive(ttlConfig);
            //key:2020-02-01-16 value:次数 最多会装2个，因为之前的已经存储到数据库了
            hourMapState = getRuntimeContext().getMapState(hourStateDescripator);
            disHourMapState = getRuntimeContext().getMapState(disHourStateDescripator);
            bloomFilterMapState = getRuntimeContext().getMapState(hourStateDescripatorBloom);
        }

        @Override
        public void processElement(ActivityBean value, Context ctx, Collector<ActivityBean> out) throws Exception {
            //按小时未去重
            String subTime = value.times.substring(0, 13);
            Long count = hourMapState.get(subTime);
            if (count == null) {
                count = 0l;
            }
            count += 1;
            hourMapState.put(subTime, count);

            //按小时去重
            BloomFilter<String> hourBloomFilter = bloomFilterMapState.get(subTime);
            Long disCount = disHourMapState.get(subTime);
            if (hourBloomFilter == null) {
                hourBloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
                disCount = 0l;
            }
            //使用布隆过滤器判断这个用户在这个小时是否存在
            if (!hourBloomFilter.mightContain(value.uid)) {
                //如果不存在就加一
                disCount += 1L;
                //并且将该uid加入到布隆过滤器
                hourBloomFilter.put(value.uid);
            }

            bloomFilterMapState.put(subTime, hourBloomFilter);
            disHourMapState.put(subTime, disCount);
            value.times = subTime;
            value.activityCount = count;
            value.activityDisCount = disCount;
            out.collect(value);
        }
    }
}
