package com.lby.chapter05.bean;

import java.sql.Timestamp;

public class Event {
    public String user;

    public String url;

    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }

    public static void main(String[] args) {
        Event bob = new Event("Bob", "./order?id=17896", System.currentTimeMillis());
        System.out.println(bob);
    }
}
