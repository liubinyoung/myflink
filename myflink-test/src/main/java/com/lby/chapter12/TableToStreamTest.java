package com.lby.chapter12;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.table.api.Expressions.$;

/**
 * @author ：lby
 * @date ：Created at 2022/5/1 22:58
 * @desc ：表转换为流和使用SQL进行简单的转换统计。
 */
public class TableToStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());
        sourceDS.print("source DS -->");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        tableEnv.createTemporaryView("ClickTable", sourceDS, $("url"), $("user"));
        Table aliceVisitTable = tableEnv.sqlQuery("select user, url from ClickTable where user = 'Alice'");
        Table urlCountTable = tableEnv.sqlQuery("select user, count(url) from ClickTable group by user");
        tableEnv.toDataStream(aliceVisitTable).print("alice visit -->");
        tableEnv.toChangelogStream(urlCountTable).print("url count -->");

        streamEnv.execute();
    }
}
