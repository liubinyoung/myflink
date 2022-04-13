package com.lby.chapter06;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * 计算周期为10s的滚动窗口的pv和uv
 * keyBy + window + aggregate(AggregateFunction)
 */
public class WindowAggregateFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());
        sourceDS.print("source");
        sourceDS
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long l) {
                                        return element.timestamp;
                                    }
                                })
                )
                .keyBy(t -> true)   // 所有数据发到一个分区
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new PvUv())
                .print("结果");

        streamEnv.execute();

    }

    public static class PvUv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Tuple2<Long, Long>>{

        /**
         * 初始化累加器
         * @return
         */
        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<String>());
        }

        /**
         * 本窗口的数据来一条累加一次，并返回累加器
         * @param input 输入数据
         * @param accumulator 累加器
         * @return
         */
        @Override
        public Tuple2<Long, HashSet<String>> add(Event input, Tuple2<Long, HashSet<String>> accumulator) {
            accumulator.f1.add(input.user);
            return Tuple2.of(accumulator.f0 + 1L, accumulator.f1);
        }

        /**
         * 窗口闭合时，增量聚合结束， 将计算结果发送到下游
         * @param accumulator
         * @return
         */
        @Override
        public Tuple2<Long, Long> getResult(Tuple2<Long, HashSet<String>> accumulator) {
            return Tuple2.of(accumulator.f0, (long) accumulator.f1.size());
        }

        /**
         * 窗口与窗口之间的merge，适用于会话窗口
         * @param longHashSetTuple2
         * @param acc1
         * @return
         */
        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }

    }
}
