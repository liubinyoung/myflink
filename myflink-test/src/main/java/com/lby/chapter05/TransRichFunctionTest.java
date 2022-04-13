package com.lby.chapter05;

import com.lby.chapter05.bean.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 富函数
 */
public class TransRichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<Event> eventDataStreamSource = streamEnv.fromElements(
                new Event("Alice", "./cart", 1000L),
                new Event("Candy", "./home", 2000L),
                new Event("Candy", "./home", 3000L)
        );

        eventDataStreamSource.map(new RichMapFunction<Event, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                RuntimeContext runtimeContext = getRuntimeContext();
//                runtimeContext.getAttemptNumber();
//                runtimeContext.getDistributedCache();
//                runtimeContext.getExecutionConfig();
//                runtimeContext.getMetricGroup();
//                runtimeContext.getMaxNumberOfParallelSubtasks();
//                runtimeContext.getNumberOfParallelSubtasks();
//                runtimeContext.getTaskName();
//                runtimeContext.getTaskNameWithSubtasks();
//                runtimeContext.getUserCodeClassLoader();
                System.out.println("JobId:" + runtimeContext.getJobId().toString() +
                        ", SubTaskIndex:" + runtimeContext.getIndexOfThisSubtask() + ", Beginning...");

            }

            @Override
            public String map(Event event) throws Exception {
                return event.user + "[" + event.url + "]";
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("JobId:" + getRuntimeContext().getJobId().toString() +
                        ", SubTaskIndex:" + getRuntimeContext().getIndexOfThisSubtask() + ", End.");
            }
        }).print();

        streamEnv.execute();
    }
}
