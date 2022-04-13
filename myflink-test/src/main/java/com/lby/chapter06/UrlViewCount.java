package com.lby.chapter06;

import java.sql.Timestamp;

public class UrlViewCount {
    private String url;
    private Long count;
    private Long start;
    private Long end;

    public UrlViewCount(String url, Long count, Long start, Long end) {
        this.url = url;
        this.count = count;
        this.start = start;
        this.end = end;
    }

    public UrlViewCount() {
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", start=" + new Timestamp(start) +
                ", end=" + new Timestamp(end) +
                '}';
    }
}
