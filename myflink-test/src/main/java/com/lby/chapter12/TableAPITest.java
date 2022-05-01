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
 * @date ：Created at 2022/4/30 22:09
 * @desc ：使用Table API + SQL测试
 */
public class TableAPITest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());

        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        // 将数据流转换为表
        Table table = tableEnv.fromDataStream(sourceDS);

        // 方式一：用执行sql的方式提取数据
        /*
        我们并没有将 Table 对象注册为虚拟表就直接在 SQL 中使用了,这其实是一种简略的写法，
        我们将 Table 对象名 eventTable 直接以字符串拼接的形式添加到 SQL 语句中，在解析时会
        自动注册一个同名的虚拟表到环境中，这样就省略了创建虚拟视图的步骤。
        */
//         Table selectTable = tableEnv.sqlQuery("select user, url  from " + table);

        // 方式二：使用DSL声明式领域特定语言
        Table selectTable = table.select($("user"), $("url"));


        // 将查询结果表转换成数据流，打印输出
        tableEnv.toDataStream(selectTable).print("TABLE-->");

        streamEnv.execute();
    }
}
