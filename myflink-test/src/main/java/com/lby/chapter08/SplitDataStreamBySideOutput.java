package com.lby.chapter08;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class SplitDataStreamBySideOutput {
    private static OutputTag<Tuple3<String, String, Long>> maryTag = new OutputTag<Tuple3<String, String, Long>>("Mary-Tag"){};
    private static OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String, String, Long>>("Bob-Tag"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> processDS = sourceDS.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, Context context, Collector<Event> collector) throws Exception {
                if (event.user.equals("Mary")) {
                    context.output(maryTag, Tuple3.of(event.user, event.url, event.timestamp));
                } else if (event.user.equals("Bob")) {
                    context.output(BobTag, Tuple3.of(event.user, event.url, event.timestamp));
                } else {
                    collector.collect(event);
                }
            }
        });

        processDS.print("else");
        processDS.getSideOutput(maryTag).print("Mary");
        processDS.getSideOutput(BobTag).print("Bob");

        streamEnv.execute();
    }
}
