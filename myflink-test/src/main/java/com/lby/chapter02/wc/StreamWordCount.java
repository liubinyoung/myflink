package com.lby.chapter02.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameters = ParameterTool.fromArgs(args);
        String hostname = parameters.get("hostname");
        int port = parameters.getInt("port");

        DataStreamSource<String> socketDS = streamEnv.socketTextStream(hostname, port);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneDS = socketDS.flatMap((String line, Collector<String> out) ->
                Arrays.stream(line.split(" ")).forEach(out::collect))
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> keyByDS = wordAndOneDS.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sumDS = keyByDS.sum(1);

        sumDS.print();

        streamEnv.execute();

    }
}
