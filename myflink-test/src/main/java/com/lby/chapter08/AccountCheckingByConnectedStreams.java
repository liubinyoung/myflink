package com.lby.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author ：lby
 * @date ：Created at 2022/4/17 22:35
 * @desc ：通过ConnectedStreams实现对账功能。
 */
public class AccountCheckingByConnectedStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        streamEnv.setParallelism(1);

        DataStreamSource<Tuple3<String, String, Long>> sourceDS1 = streamEnv.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 3000L)
        );
        DataStreamSource<Tuple4<String, String, String, Long>> sourceDS2 = streamEnv.fromElements(
                Tuple4.of("order-1", "third-party", "success", 1000L),
                Tuple4.of("order-2", "third-party", "success", 2000L)
        );
        SingleOutputStreamOperator<Tuple3<String, String, Long>> oneStream = sourceDS1
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long l) {
                        return element.f2;
                    }
                })
        );

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> twoStream = sourceDS2
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, String, String, Long> element, long l) {
                        return element.f3;
                    }
                })
        );

        oneStream.connect(twoStream)
                .keyBy(t3 -> t3.f0, t4 -> t4.f0)
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>() {
                    // 创建app值状态和第三方支付平台值状态
                    private ValueState<Tuple3<String, String, Long>> appValueState;
                    private ValueState<Tuple4<String, String, String, Long>> thirdPartyValueState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        appValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple3<String, String, Long>>(
                                        "app",
                                        Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)
                                )
                        );
                        thirdPartyValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple4<String, String, String, Long>>(
                                        "third-party",
                                        Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)
                                )
                        );
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> element1, Context context, Collector<String> collector) throws Exception {
                        // thirdPartyValueState 不为空，则说明第三方支付平台数据先到，对账成功，清空状态
                        if (thirdPartyValueState.value() != null) {
                            collector.collect("对账成功！" + "app数据：" + element1.toString()
                                    + "，第三方支付平台数据：" + thirdPartyValueState.value().toString());
                            thirdPartyValueState.clear();
                        } else {  // thirdPartyValueState 为空，则将数据加入状态并等对方5秒，注册5秒定时器
                            appValueState.update(element1);
                            context.timerService().registerEventTimeTimer(element1.f2 + 5 * 1000L);
                        }
                    }

                    @Override
                    public void processElement2(Tuple4<String, String, String, Long> element2, Context context, Collector<String> collector) throws Exception {
                        // appValueState 不为空，则说明第三方支付平台数据先到，对账成功，清空状态
                        if (appValueState.value() != null) {
                            collector.collect("对账成功！" + "第三方支付平台数据：" + element2.toString()
                                    + "，app数据：" + appValueState.value().toString());
                            appValueState.clear();
                        } else {  // thirdPartyValueState 为空，则等对方5秒，注册5秒定时器
                            thirdPartyValueState.update(element2);
                            context.timerService().registerEventTimeTimer(element2.f3 + 5 * 1000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器触发，如果状态还没被清空，说明等了5秒期间还没有对账成功
                        if (appValueState.value() != null) {
                            out.collect("对账失败！app数据：" + appValueState.value() + "，无第三方支付平台数据。");
                        } else if (thirdPartyValueState.value() != null) {
                            out.collect("对账失败！第三方支付平台数据：" + thirdPartyValueState.value() + "，无app数据。");
                        }
                        appValueState.clear();
                        thirdPartyValueState.clear();
                    }
                }).print();
//                .process(new KeyedCoProcessFunction<String, Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>() {
//                    // 创建app值状态和第三方支付平台值状态
//                    private ValueState<Tuple3<String, String, Long>> appValueState;
//                    private ValueState<Tuple4<String, String, String, Long>> thirdPartyValueState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        appValueState = getRuntimeContext().getState(
//                                new ValueStateDescriptor<Tuple3<String, String, Long>>(
//                                        "app",
//                                        Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)
//                                )
//                        );
//                        thirdPartyValueState = getRuntimeContext().getState(
//                                new ValueStateDescriptor<Tuple4<String, String, String, Long>>(
//                                        "third-party",
//                                        Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)
//                                )
//                        );
//                    }
//
//                    @Override
//                    public void processElement1(Tuple3<String, String, Long> element1, Context context,
//                                                Collector<String> collector) throws Exception {
//                        // thirdPartyValueState 不为空，则说明第三方支付平台数据先到，对账成功，清空状态
//                        if (thirdPartyValueState != null) {
//                            collector.collect("对账成功！" + "app数据：" + element1.toString()
//                                    + "，第三方支付平台数据：" + thirdPartyValueState.value().toString());
//                            thirdPartyValueState.clear();
//                        } else {  // thirdPartyValueState 为空，则将数据加入状态并等对方5秒，注册5秒定时器
//                            appValueState.update(element1);
//                            context.timerService().registerEventTimeTimer(element1.f2 + 5 * 1000L);
//                        }
//                    }
//
//                    @Override
//                    public void processElement2(Tuple4<String, String, String, Long> element2,
//                                                Context context, Collector<String> collector) throws Exception {
//                        // appValueState 不为空，则说明第三方支付平台数据先到，对账成功，清空状态
//                        if (appValueState != null) {
//                            collector.collect("对账成功！" + "第三方支付平台数据：" + element2.toString()
//                                    + "，app数据：" + appValueState.value().toString());
//                            appValueState.clear();
//                        } else {  // thirdPartyValueState 为空，则等对方5秒，注册5秒定时器
//                            thirdPartyValueState.update(element2);
//                            context.timerService().registerEventTimeTimer(element2.f3 + 5 * 1000L);
//                        }
//                    }
//
//                    @Override
//                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
//                        // 定时器触发，如果状态还没被清空，说明等了5秒期间还没有对账成功
//                        if (appValueState != null) {
//                            out.collect("对账失败！app数据：" + appValueState.value() + "，无第三方支付平台数据。");
//                        } else if (thirdPartyValueState != null) {
//                            out.collect("对账失败！第三方支付平台数据：" + thirdPartyValueState.value() + "，无app数据。");
//                        }
//
//                    }
//                }).print("result");
        streamEnv.execute();
    }
}
