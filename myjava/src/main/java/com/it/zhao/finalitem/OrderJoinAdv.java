package com.it.zhao.finalitem;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.*;


public class OrderJoinAdv {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        FlinkUtils.env.setParallelism(1);
        FlinkUtils.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> orderDetail = FlinkUtils.createKafkaStream(parameterTool, SimpleStringSchema.class, "orderdetail");
        DataStream<String> orderMain = FlinkUtils.createKafkaStream(parameterTool, SimpleStringSchema.class, "ordermain");

        SingleOutputStreamOperator<OrderMain> orderMainProcess = orderMain.process(new ProcessFunction<String, OrderMain>() {
            @Override
            public void processElement(String value, Context ctx, Collector<OrderMain> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String jsonObjectString = jsonObject.getString("type");
                    if (jsonObjectString.equals("INSERT") || jsonObjectString.equals("UPDATE")) {
                        JSONArray jsonArray = jsonObject.getJSONArray("data");
                        for (int i = 0; i < jsonArray.size(); i++) {
                            OrderMain object = jsonArray.getObject(i, OrderMain.class);
                            out.collect(object);
                        }
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                }
            }
        });


        SingleOutputStreamOperator<OrderDetail> orderDetailProcess = orderDetail.process(new ProcessFunction<String, OrderDetail>() {
            @Override
            public void processElement(String value, Context ctx, Collector<OrderDetail> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String jsonObjectString = jsonObject.getString("type");
                    if (jsonObjectString.equals("INSERT") || jsonObjectString.equals("UPDATE")) {
                        JSONArray jsonArray = jsonObject.getJSONArray("data");
                        for (int i = 0; i < jsonArray.size(); i++) {
                            OrderDetail object = jsonArray.getObject(i, OrderDetail.class);
                            out.collect(object);
                        }
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                }
            }
        });


        SingleOutputStreamOperator<OrderDetail> orderDetailSingleOutputStreamOperator = orderDetailProcess.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderDetail>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(OrderDetail element) {
                return element.getCreate_time().getTime();
            }
        });

        OutputTag<OrderDetail> detailOutputTag = new OutputTag<OrderDetail>("late-data") {
        };
        SingleOutputStreamOperator<OrderDetail> orderDetailApply = orderDetailSingleOutputStreamOperator.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(detailOutputTag)
                .apply(new AllWindowFunction<OrderDetail, OrderDetail, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<OrderDetail> values, Collector<OrderDetail> out) throws Exception {
                        for (OrderDetail value : values) {
                            out.collect(value);
                        }
                    }
                });

        DataStream<OrderDetail> sideOutput = orderDetailApply.getSideOutput(detailOutputTag);
        SingleOutputStreamOperator<Tuple2<OrderDetail, OrderMain>> late = sideOutput.map(new MapFunction<OrderDetail, Tuple2<OrderDetail, OrderMain>>() {
            @Override
            public Tuple2<OrderDetail, OrderMain> map(OrderDetail orderDetail) throws Exception {
                return Tuple2.of(orderDetail, null);
            }
        });

        SingleOutputStreamOperator<OrderMain> orderMainSingleOutputStreamOperator = orderMainProcess.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderMain>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(OrderMain element) {
                return element.getCreate_time().getTime();
            }
        });

        DataStream<Tuple2<OrderDetail, OrderMain>> dataStream = orderDetailApply.coGroup(orderMainSingleOutputStreamOperator)
                .where(new KeySelector<OrderDetail, Integer>() {
                    @Override
                    public Integer getKey(OrderDetail orderDetail) throws Exception {
                        return orderDetail.getOrder_id();
                    }
                }).equalTo(new KeySelector<OrderMain, Integer>() {
                    @Override
                    public Integer getKey(OrderMain orderMain) throws Exception {
                        return orderMain.getOid();
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<OrderDetail, OrderMain, Tuple2<OrderDetail, OrderMain>>() {
                    @Override
                    public void coGroup(Iterable<OrderDetail> first, Iterable<OrderMain> second, Collector<Tuple2<OrderDetail, OrderMain>> collector) throws Exception {
                        for (OrderDetail detail : first) {
                            boolean flag = false;
                            for (OrderMain main : second) {
                                collector.collect(Tuple2.of(detail, main));
                                flag = true;
                            }
                            if (!flag) {
                                collector.collect(Tuple2.of(detail, null));
                            }
                        }
                    }
                });
//        dataStream.print();
//        late.print("late: ");

        dataStream.map(new RichMapFunction<Tuple2<OrderDetail, OrderMain>, Tuple2<OrderDetail, OrderMain>>() {
            private Connection conn;
            private PreparedStatement ppst;

            @Override
            public void open(Configuration parameters) throws Exception {
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/learn?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC", "root", "1234");
            }

            @Override
            public Tuple2<OrderDetail, OrderMain> map(Tuple2<OrderDetail, OrderMain> value) throws Exception {
                if (value.f1 == null) {
                    value.f1 = queryEelement(conn, value.f0.getOrder_id(), ppst);
                }
                return value;
            }

            @Override
            public void close() throws Exception {
                if (ppst != null) {
                    ppst.close();
                }
                if (conn != null) {
                    conn.close();
                }
            }
        }).print();
//
//
        FlinkUtils.env.execute();
    }

    private static OrderMain queryEelement(Connection conn, int order_id, PreparedStatement ppst) throws SQLException {
        ppst = conn.prepareStatement("select * from ordermain where oid = ?");
        ppst.setInt(1, order_id);
        ResultSet resultSet = ppst.executeQuery();
        int oid = resultSet.getInt("oid");
        double total_money = resultSet.getDouble("total_money");
        OrderMain orderMain = new OrderMain();
        orderMain.setOid(oid);
        orderMain.setTotal_money(total_money);
        orderMain.setStatus(15);
        return orderMain;
    }
}

