package com.lby.chapter06;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * 统计每10秒钟的UV
 * keyBy + window + process(ProcessWindowFunction)
 *
 * 全窗口函数：先收集数据缓存起来，等到需要输出结果时才开始计算。
 *      WindowFunction: 提供的上下文信息较少，功能被ProcessWindowFunction全覆盖，以后可能会被逐渐弃用;
 *      ProcessWindowFunction: 能够获取窗口信息，还可以访问的当前的时间和状态信息，更加灵活.
 * 全窗口函数因为运行效率较低，很少单独使用。往往会和增量聚合函数结合在一起，共同实现窗口的处理计算。
 *
 */
public class ProcessWindowFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());
        sourceDS.print("sourceDS");
        sourceDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }))
                .keyBy(event -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Event, String, Boolean, TimeWindow>() {

                    @Override
                    public void process(Boolean aBoolean, Context context, Iterable<Event> elems,
                                        Collector<String> collector) throws Exception {
                        HashSet<String> users = new HashSet<>();
                        for (Event elem : elems) {
                            users.add(elem.user);
                        }
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        collector.collect("窗口" + new Timestamp(start) + "~" + new Timestamp(end)
                                + " UV值为：" + users.size());
                    }
                }).print("OUTPUT");
        streamEnv.execute();
    }
}
