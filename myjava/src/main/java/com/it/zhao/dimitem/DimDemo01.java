package com.it.zhao.dimitem;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DimDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> line = env.socketTextStream("hadoop101", 8888);
        //做变换，转化成bean结构，解析json
        SingleOutputStreamOperator<LogBean> log = line.map(new MapFunction<String, LogBean>() {
            @Override
            public LogBean map(String s) throws Exception {
                LogBean logBean = null;
                try {
                    logBean = JSON.parseObject(s, LogBean.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return logBean;
            }
        });
        //过滤无效数据
        SingleOutputStreamOperator<LogBean> filter = log.filter(new FilterFunction<LogBean>() {
            @Override
            public boolean filter(LogBean logBean) throws Exception {
                return logBean != null;
            }
        });
        //关联维度信息
        SingleOutputStreamOperator<LogBean> ds2 = filter.map(new RichMapFunction<LogBean, LogBean>() {
            private transient Connection conn;
            private transient PreparedStatement ppst;

            //注册mysql链接
            @Override
            public void open(Configuration parameters) throws Exception {
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/learn?useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC", "root", "1234");
                Class.forName("com.mysql.cj.jdbc.Driver");
                ppst = conn.prepareStatement("select id,name from dim_catagary where id = ?");
            }

            //释放资源
            @Override
            public void close() throws Exception {
                if (ppst != null) {
                    ppst.close();
                }
                if (conn != null) {
                    conn.close();
                }
            }

            //mysql处理逻辑
            @Override
            public LogBean map(LogBean logBean) throws Exception {
                ppst.setInt(1, logBean.cid);
                ResultSet resultSet = ppst.executeQuery();
                String name = null;
                if (resultSet.next()) {
                    name = resultSet.getString(2);
                }
                resultSet.close();
                logBean.name = name;
                return logBean;
            }
        });

        SingleOutputStreamOperator<LogBean> map = ds2.map(new GeoRichMapFunction("4924f7ef5c86a278f5500851541cdcff"));
        map.print();

        //filter.print();
        env.execute();
    }
}
