package edu;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.util.internal.JacksonUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class TimestampAndTableNameInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();

        String log = new String(event.getBody(), StandardCharsets.UTF_8);
        JSONObject logJson = JSONObject.parseObject(log);

        Long timeStamp = logJson.getLong("ts");
        String table = logJson.getJSONObject("source").getString("table");

        headers.put("timestamp", String.valueOf(timeStamp));
        headers.put("tableName", table);
        
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public void configure(Context context) {

        }

        @Override
        public Interceptor build() {
            return new TimestampAndTableNameInterceptor();
        }
    }
}
