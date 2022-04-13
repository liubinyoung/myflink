package com.lby.chapter05;

import com.lby.chapter05.bean.Event;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 计算访问频次最高的用户的姓名和次数
 *      1）计算每个用户的访问频次；
 *      2）根据1）计算的结果取最大频次的数据；
 */
public class TransReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        DataStreamSource<Event> eventDataStreamSource = streamEnv.fromElements(
                new Event("Alice", "./cart", 1000L),
                new Event("Candy", "./home", 2000L),
                new Event("Candy", "./home", 3000L),
                new Event("Bob", "./order?id=18976", 4000L),
                new Event("Alice", "./cart", 5000L),
                new Event("Alice", "./cart", 6000L)

        );

//        eventDataStreamSource.print();

        SingleOutputStreamOperator<Tuple2<String, Long>> mapDS = eventDataStreamSource.map(event -> Tuple2.of(event.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        mapDS.keyBy(t -> t.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        return Tuple2.of(t1.f0, t1.f1 + t2.f1);
                    }
                })
                .keyBy(t -> true)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        return t1.f1 > t2.f1 ? t1 : t2;
                    }
                }).print();

        streamEnv.execute();
    }
}
