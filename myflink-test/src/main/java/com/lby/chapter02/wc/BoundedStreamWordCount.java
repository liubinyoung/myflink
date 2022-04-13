package com.lby.chapter02.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lineDS = streamEnv.readTextFile("input/words.txt");

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneDS = lineDS.flatMap(
                (String line, Collector<String> out) ->
                    Arrays.stream(line.split(" ")).forEach(out::collect)
        ).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> keyByDS = wordAndOneDS.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sumDS = keyByDS.sum(1);

        sumDS.print();

        streamEnv.execute();

    }
}
