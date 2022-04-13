package com.lby.chapter05;

import com.lby.chapter05.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class ClickSource implements SourceFunction<Event> {

    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();
        String[] users = {"Alice", "Bob", "Candy", "Mary"};
        String[] urls = {"./home", "./order", "./detail", "./fav"};
        while (running) {
            Event event = new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    System.currentTimeMillis()
            );
            sourceContext.collect(event);
            // 每隔1000毫秒生成一条数据
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
