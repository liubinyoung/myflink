package com.lby.chapter05;

import com.lby.chapter05.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment steamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> sourceDS = steamEnv.addSource(new ClickSource());
        sourceDS.print();
        steamEnv.execute();
    }
}
