package com.lby.chapter06;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * 增量聚合和全窗口函数结合使用，计算每10s钟每个url的PV，并附带窗口信息输出
 * keyBy + window + aggregate(AggregateFunction + ProcessWindowFunction)
 *
 * 全窗口函数因为运行效率较低，很少单独使用。往往会和增量聚合函数结合在一起，共同实现窗口的处理计算。
 */
public class AggregateAndProcessWindowFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());
        sourceDS.print("source");
        sourceDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {

                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }))
                .map(event -> Tuple2.of(event.url,1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new PvByAggregate(), new WindowInfoByProcessWindow())
                .print("Aggregate");

        streamEnv.execute();
    }

    public static class PvByAggregate implements AggregateFunction<Tuple2<String, Long>, Long, Long> {


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> t1, Long accum) {
            return accum + 1L;
        }

        @Override
        public Long getResult(Long accum) {
            return accum;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    public static class WindowInfoByProcessWindow extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        @Override
        public void process(String url, Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new UrlViewCount(url, count, start, end).toString());
        }
    }


}
