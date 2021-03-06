package com.lby.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


import java.time.Duration;

/**
 * 通过union算子进行流的合并。
 * 可以对多条流进行合并，而且数据类型必须相同，所以灵活性不足。
 * 合并之后，水位线以时间慢的为准。
 */
public class UnionDataStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        SingleOutputStreamOperator<String> socketDS1 = streamEnv.socketTextStream("lby01", 7777)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String s, long l) {
                                return Long.valueOf(s.split(",")[2]);
                            }
                        }));
        SingleOutputStreamOperator<String> socketDS2 = streamEnv.socketTextStream("lby01", 8888)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String s, long l) {
                                return Long.valueOf(s.split(",")[2]);
                            }
                        }));

        socketDS1.union(socketDS2)
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                        Long currentWatermark = context.timerService().currentWatermark();
                        collector.collect("水位线：" + currentWatermark.toString());
                    }
                }).print();


        streamEnv.execute();
    }
}
