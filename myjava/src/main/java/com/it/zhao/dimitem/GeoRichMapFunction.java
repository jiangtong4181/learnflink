package com.it.zhao.dimitem;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


public class GeoRichMapFunction extends RichMapFunction<LogBean, LogBean> {
    private String key;

    public GeoRichMapFunction(String key) {
        this.key = key;
    }

    private transient CloseableHttpClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        client = HttpClients.createDefault();
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    //通过http请求查询高德地图，省份城市
    @Override
    public LogBean map(LogBean logBean) throws Exception {
        Double longtitude = logBean.longtitude;
        Double latitude = logBean.latitude;
        HttpGet httpGet = new HttpGet("https://restapi.amap.com/v3/geocode/regeo?&location=" + longtitude + "," + latitude + "&key=" + key);
        CloseableHttpResponse execute = client.execute(httpGet);
        try {
            HttpEntity entity = execute.getEntity();
            String province = null;
            String city = null;
            if (execute.getStatusLine().getStatusCode() == 200) {
                String line = EntityUtils.toString(entity);
                JSONObject jsonObject = JSON.parseObject(line);
                JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                if (regeocode != null && !regeocode.isEmpty()) {
                    JSONObject addressComponent = regeocode.getJSONObject("addressComponent");
                    logBean.province = addressComponent.getString("province");
                    logBean.city = addressComponent.getString("city");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            execute.close();
        }
        return logBean;
    }
}
