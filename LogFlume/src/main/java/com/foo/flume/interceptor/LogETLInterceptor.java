package com.foo.flume.interceptor;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.velocity.runtime.directive.Foreach;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取数据
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));
        if(log.contains("start")){
            if(LogUtils.validateStart(log)){
                return event;
            }
        }else {
            if(LogUtils.validateEvent("event")){
                return event;
            }
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> interceptos = new ArrayList<>();
        for (Event event : list) {
            Event intercept1 = intercept(event);
            if(intercept1 != null){
                interceptos.add(intercept1);
            }
        }
        return interceptos;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
