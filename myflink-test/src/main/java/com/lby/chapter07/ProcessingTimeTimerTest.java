package com.lby.chapter07;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 处理事件定时器测试
 * 只有KeyedStream才支持设置定时器的操作
 */
public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());
        sourceDS.keyBy(event -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 10 * 1000L);
                        out.collect("输入数据：" + value.toString() + "，当前处理时间：" + new Timestamp(currentProcessingTime));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("处理时间定时器触发，时间为：" + new Timestamp(timestamp));

                    }
                }).print();

        streamEnv.execute();
    }
}
