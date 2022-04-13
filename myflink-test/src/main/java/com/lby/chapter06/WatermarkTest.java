package com.lby.chapter06;

import com.lby.chapter05.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<String> sourceDS = streamEnv.socketTextStream("lby01", 8888);
        sourceDS.map(new MapFunction<String, Event>() {

            @Override
            public Event map(String value) throws Exception {
                String[] splits = value.split(",");
                return new Event(splits[0], splits[1], Long.valueOf(splits[2]));
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }))
                .keyBy(event -> event.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
                        long windowStart = context.window().getStart();
                        long windowEnd = context.window().getEnd();
                        long exactSizeIfKnown = iterable.spliterator().getExactSizeIfKnown();
                        collector.collect(new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd) +
                                " 窗口每个用户的PV为：" + exactSizeIfKnown);
                    }
                }).print();

        streamEnv.execute();
    }
}
