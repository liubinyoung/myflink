package com.lby.chapter07;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

public class ProcessAllWindowFunctionTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());
        sourceDS.print();

        sourceDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }))
                .map(event -> event.url).returns(Types.STRING)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {

                    @Override
                    public void process(Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        // 将数据使用HashMap存起来
                        HashMap<String, Long> urlHashMap = new HashMap<>();
                        Iterator<String> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            String next = iterator.next();
                            if (urlHashMap.containsKey(next)) {
                                urlHashMap.put(next, urlHashMap.get(next) + 1L);
                            } else {
                                urlHashMap.put(next, 1L);
                            }
                        }
                        ArrayList<Tuple2<String, Long>> urlList = new ArrayList<>();
                        for (String key : urlHashMap.keySet()) {
                            urlList.add(Tuple2.of(key, urlHashMap.get(key)));
                        }

                        Collections.sort(urlList, new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                                return o2.f1.intValue() - o1.f1.intValue();
                            }
                        });

                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long size = elements.spliterator().getExactSizeIfKnown();
                        for (int i = 0; i < (2 > size ? size : 2) ; i++) {
                            out.collect( "Top" + (i + 1) + "=> url为: " + urlList.get(i).f0 + "，数量为："
                                    + urlList.get(i).f1 + ", 窗口为：" + new Timestamp(start) + " ~ " + new Timestamp(end));
                        }
                    }

                    @Override
                    public void clear(Context context) throws Exception {
                        super.clear(context);
                    }
                }).print();
        streamEnv.execute();
    }
}
