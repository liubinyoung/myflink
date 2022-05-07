package com.lby.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class SuspiciouslyLoginDetectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        SingleOutputStreamOperator<LoginEventBean> sourceDS = streamEnv.fromElements(
                new LoginEventBean("user-1", "ip-1", "fail", 1000L),
                new LoginEventBean("user-1", "ip-1", "fail", 2000L),
                new LoginEventBean("user-2", "ip-2", "fail", 3000L),
                new LoginEventBean("user-2", "ip-3", "fail", 4000L),
                new LoginEventBean("user-2", "ip-4", "success", 5000L),
                new LoginEventBean("user-1", "ip-5", "fail", 6000L),
                new LoginEventBean("user-1", "ip-6", "fail", 7000L),
                new LoginEventBean("user-1", "ip-7", "fail", 8000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEventBean>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEventBean>() {
                            @Override
                            public long extractTimestamp(LoginEventBean element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
        );
        sourceDS.print("source-->");

        KeyedStream<LoginEventBean, String> keyedDS = sourceDS.keyBy(event -> event.getUserId());

        // 1.定义Pattern，连续的三个登录失败事件
        // 每一个简单事件并不是任意选取的，也需要有一定的条件规则；所以我们就把每个简单事件的匹配规则，叫作“个体模式”（Individual Pattern）。
        Pattern<LoginEventBean, LoginEventBean> definePattern = Pattern.<LoginEventBean>begin("first")
                .where(new SimpleCondition<LoginEventBean>() {
                    @Override
                    public boolean filter(LoginEventBean value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .next("second")  // next表示紧挨着，中间不能有其他类型事件
                .where(new SimpleCondition<LoginEventBean>() {
                    @Override
                    public boolean filter(LoginEventBean value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .next("third")
                .where(new SimpleCondition<LoginEventBean>() {
                    @Override
                    public boolean filter(LoginEventBean value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                });
        // 2.将Pattern应用到流上，匹配符合复杂事件组合的数据，得到一个PatternStream
        PatternStream<LoginEventBean> patternStream = CEP.pattern(keyedDS, definePattern);

        // 3.将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出
        patternStream.select(new PatternSelectFunction<LoginEventBean, String>() {
            @Override
            public String select(Map<String, List<LoginEventBean>> map) throws Exception {
                // map中存放的是匹配到的复杂事件，key对应事件的名称
                LoginEventBean first = map.get("first").get(0);
                LoginEventBean second = map.get("second").get(0);
                LoginEventBean third = map.get("third").get(0);
                return first.getUserId() + "连续三次登录失败，" + "登录事件分别为："
                        + first.getTimestamp() + "," + second.getTimestamp() + "," + third.getTimestamp();
            }
        }).print();

        streamEnv.execute();
    }
}
