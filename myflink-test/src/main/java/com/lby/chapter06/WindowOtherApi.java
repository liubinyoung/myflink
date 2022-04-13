package com.lby.chapter06;

import com.lby.chapter05.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 测试窗口迟到数据的三重保障：乱序程度 + 窗口允许迟到 + 测输出流
 */
public class WindowOtherApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<String> sourceDS = streamEnv.socketTextStream("lby01", 9999);
        sourceDS.print("source->");
        OutputTag<Event> late = new OutputTag<Event>("LATE") {};

        SingleOutputStreamOperator<String> aggregateDS = sourceDS.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                String[] splits = value.split(",");
                return new Event(splits[0], splits[1], Long.valueOf(splits[2]));
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }))
                .keyBy(event -> event.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(10))
                .sideOutputLateData(late)
                .aggregate(new MyAggFunction(), new MyProcessWindowFunction());

        aggregateDS.print("aggregate-->");
        aggregateDS.getSideOutput(late).print("late--->");

        streamEnv.execute();
    }

    private static class MyAggFunction implements AggregateFunction<Event, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    private static class MyProcessWindowFunction extends ProcessWindowFunction<Long, String, String, TimeWindow> {

        @Override
        public void process(String user, Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long pv = iterable.iterator().next();
            collector.collect(start + "~" + end + " " + user + "pv为：" + pv);
        }
    }
}
