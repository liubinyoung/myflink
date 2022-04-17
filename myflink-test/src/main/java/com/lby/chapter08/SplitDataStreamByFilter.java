package com.lby.chapter08;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：lby
 * @date ：Created at 2022/4/17 17:49
 * @desc ：通过 filter 进行分流; split()分流方法已经被淘汰了。
 */
public class SplitDataStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());
        sourceDS.print("source--");

        // 使用fliter进行分流
        SingleOutputStreamOperator<Event> streamAlice = sourceDS.filter(event -> event.user.equals("Alice"));
        SingleOutputStreamOperator<Event> streamBob = sourceDS.filter(event -> event.user.equals("Bob"));
        SingleOutputStreamOperator<Event> streamOthers
                = sourceDS.filter(event -> !(event.user.equals("Alice") || event.user.equals("Bob")));

        streamAlice.print("Alice Result==");
        streamBob.print("Bob Result==");
        streamOthers.print("Others Result==");

        streamEnv.execute();
    }
}
