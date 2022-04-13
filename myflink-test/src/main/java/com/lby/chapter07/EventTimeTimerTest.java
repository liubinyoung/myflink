package com.lby.chapter07;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;
import java.time.Duration;

/**
 * 测试事件时间定时器
 */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        streamEnv.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }))
                .keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {

                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        Long timestamp = ctx.timestamp();
                        long currentWatermark = ctx.timerService().currentWatermark();
                        ctx.timerService().registerEventTimeTimer(timestamp + 5 * 1000L);
                        out.collect("数据到达：" + value.toString());
                        out.collect("事件时间：" + new Timestamp(timestamp) + "，水位线：" + new Timestamp(currentWatermark));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("事件时间定时器触发，事件时间为：" + new Timestamp(timestamp) + "，水位线为："
                                + new Timestamp(ctx.timerService().currentWatermark()));
                    }
                }).print();

        streamEnv.execute();

    }
}
