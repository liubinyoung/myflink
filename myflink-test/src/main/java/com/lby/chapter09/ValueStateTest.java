package com.lby.chapter09;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author ：lby
 * @date ：Created at 2022/4/30 11:00
 * @desc ：值状态测试
 */
public class ValueStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());
        sourceDS.print();
        SingleOutputStreamOperator<Event> eventSOSO = sourceDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                })
        );

        eventSOSO.keyBy(event -> event.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private ValueState<Long> count;
                    private ValueState<Long> tsState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        count = getRuntimeContext()
                                .getState(new ValueStateDescriptor<Long>("count_num", Long.class));
                        tsState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<Long>("timer_ts", Long.class));
                    }

                    @Override
                    public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
                        // 如果计数状态为null，则加1为1，非空后，可在原值基础上加1
                        if (count.value() == null) {
                            count.update(1L);
                        } else {
                            count.update(count.value() + 1L);
                        }

                        if (tsState.value() == null) {
                            long registerEventTime = event.timestamp + 10 * 1000L;
                            tsState.update(registerEventTime);
                            context.timerService().registerEventTimeTimer(tsState.value());
                            System.out.println("注册定时器成功。key:" + context.getCurrentKey() + ",EventTime:"
                                    + new Timestamp(registerEventTime));
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器触发时，输出结果，并清空记录定时器注册时间的状态tsState
                        out.collect("结果输出，时间：" + new Timestamp(timestamp) + "，key为："
                                + ctx.getCurrentKey() + "，累计值为：" + count.value());
                        tsState.clear();
                    }
                }).print();

        streamEnv.execute();

    }
}
