package com.lby.chapter11;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableAPITest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);

        Table table = streamTableEnv.fromDataStream(sourceDS);

        // 方式一
        Table queryTable = streamTableEnv.sqlQuery("select user, url from " + table);
        streamTableEnv.toDataStream(queryTable).print("111");

        // 方式二
        Table dslTable = table.select($("user"), $("url")).where($("user").isEqual("Bob"));
        streamTableEnv.toDataStream(dslTable).print("222");
        streamEnv.execute();
    }
}
