package com.lby.chapter07;

import com.lby.chapter05.ClickSource;
import com.lby.chapter05.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 处理函数测试
 * ProcessFunction不是一个接口，是抽象类，继承了AbstractRichFunction
 * 有两个方法
 *      一个是抽象方法processElement(): 每个元素都会调用一次
 *      一个是非抽象方法onTimer(): 定时器触发的时候才会调用
 * ProcessFunction有以下功能：
 *      1. 提供了一个定时服务timeservice，可以通过他访问流中的事件、时间戳、水位线、注册定时事件；
 *      2. 继承了富函数AbstractRichFunction，可以访问状态和其他运行时信息；
 *      3. 可以将数据输出到侧输出流；
 *
 * Flink提供了8中不同的处理函数：
 *      (1) ProcessFunction
 *          最基本的处理函数，基于DataStream调用.process()时作为参数传入;
 *      (2) KeyedProcessFunction
 *          对流按键分区后的处理函数，基于KeyedStream调用.process()时作为参数传入;要想使用定时器，必须基于KeyedStream;
 *      (3) ProcessWindowFunction
 *          开窗之后的处理函数，也是全窗口函数的代表。基于WindowedStream调用.process()时作为参数传入;
 *      (4) ProcessAllWindowFunction
 *          同样时开窗之后的处理函数，基于AllWindowedStream调用.process()时作为参数传入;
 *      (5) CoProcessFunction
 *          合并(connect)两条流之后的处理函数，基于ConnectedStreams调用.process()时作为参数传入;
 *      (6) ProcessJoinFunction
 *          间隔连接(interval join)两条流之后的处理函数，基于IntervalJoined调用.process()时作为参数传入;
 *      (7) BroadcastProcessFunction
 *          广播连接流处理函数，基于BroadcastConnectedStream调用.process()时作为参数传入;
 *          这里的广播连接流是一个未keyBy的普通DataStream与广播流BroadcastStream连接之后的产物;
 *      (8) KeyedBroadcastProcessFunction
 *          按键分区的广播连接流处理函数，基于BroadcastConnectedStream调用.process()时作为参数传入;
 *          与BroadcastProcessFunction不同的是，这是的广播连接流是KeyedStream与广播流BroadcastStream连接之后的产物;
 *
 */
public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<Event> sourceDS = streamEnv.addSource(new ClickSource());

        sourceDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }))
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        long currentWatermark = ctx.timerService().currentWatermark();
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        Long timestamp = ctx.timestamp();
                        String timeStr = "currentWatermark: " + new Timestamp(currentWatermark)
                                + ", currentProcessingTime: " + new Timestamp(currentProcessingTime)
                                + ", timestamp: " + new Timestamp(timestamp);
                        if (value.user.equals("Alice")) {
                            out.collect(value.toString() + " => " + timeStr);
                        } else {
                            out.collect(value.toString() + ", currentWatermark: " + new Timestamp(currentWatermark));
                        }
                    }
                }).print("process");

        streamEnv.execute();
    }
}
