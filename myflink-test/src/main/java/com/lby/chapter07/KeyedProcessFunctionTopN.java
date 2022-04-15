package com.lby.chapter07;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import com.lby.chapter06.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * 使用KeyedProcessFunction计算最近10秒TopN
 *      1.按url分组开窗计算最近10秒每个url的访问次数，也就是pv
 *      2.再按窗口分组（可以按窗口开始时间或者窗口结束时间），使用KeyedProcessFunction计算TopN
 */
public class KeyedProcessFunctionTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        streamEnv.setParallelism(1);
        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());
        sourceDS.print();

        SingleOutputStreamOperator<UrlViewCount> urlCount = sourceDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }))
                .keyBy(event -> event.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                        new AggregateFunction<Event, Long, Long>() {
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
                        },
                        new ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>() {
                            @Override
                            public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                Long count = elements.iterator().next();
                                out.collect(new UrlViewCount(url, count, start, end));
                            }
                        });

        urlCount.keyBy(urlViewCount -> urlViewCount.getEnd())
                .process(new MyTopN())
                .print();

        streamEnv.execute();
    }

    private static class MyTopN extends KeyedProcessFunction<Long, UrlViewCount, String> {
        private int num;    // 前num名
        private ListState<UrlViewCount> urlCountList;

        public MyTopN() {
            this.num = 2;   // 默认是Top2
        }

        public MyTopN(int num) {
            this.num = num;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            urlCountList = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("urlCount", Types.POJO(UrlViewCount.class)));
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 将UrlViewCount数据加入状态
            urlCountList.add(value);
            // 注册事件时间定时器
            ctx.timerService().registerEventTimeTimer(value.getEnd());

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 将状态中的数据取出来
            ArrayList<UrlViewCount> resultList = new ArrayList<>();
            for (UrlViewCount urlViewCount : urlCountList.get()) {
                resultList.add(urlViewCount);
            }
            // 清空状态
            urlCountList.clear();
            // 处理取出来的数据
            resultList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });

            StringBuffer stringBuffer = new StringBuffer();
            int N = resultList.size() > num ? num : resultList.size();

            Long windowEnd = ctx.getCurrentKey();
            stringBuffer.append("----------------------------\n" + "窗口："
                    + new Timestamp(windowEnd - 10 * 1000L) + " ~ " + new Timestamp(windowEnd) + "\n") ;
            String url = "";
            Long count = 0L;
            for (int i = 0; i < num; i++) {
                if (i < resultList.size()) {
                    UrlViewCount topI = resultList.get(i);
                    url = topI.getUrl();
                    count = topI.getCount();
                } else {
                    url = "无";
                    count = 0L;
                }
                stringBuffer.append("Top" + (i + 1) + "  url:" + url + "  count:" + count + "\n");
            }
            stringBuffer.append("----------------------------\n");
            out.collect(stringBuffer.toString());
        }

    }
}
