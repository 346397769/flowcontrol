package com.example.flowcontrol.entity;

public class FlControlBean {
    //维度
    private String dimension;
    //固定时间内可以请求的次数
    private Long maxVisitValue;
    //流控固定时间长度 毫秒
    private Integer flTimeSpanMS;

    @Override
    public String toString() {
        return "FlControlBean{" +
                "dimension='" + dimension + '\'' +
                ", maxVisitValue=" + maxVisitValue +
                ", flTimeSpanMS=" + flTimeSpanMS +
                '}';
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public Long getMaxVisitValue() {
        return maxVisitValue;
    }

    public void setMaxVisitValue(Long maxVisitValue) {
        this.maxVisitValue = maxVisitValue;
    }

    public Integer getFlTimeSpanMS() {
        return flTimeSpanMS;
    }

    public void setFlTimeSpanMS(Integer flTimeSpanMS) {
        this.flTimeSpanMS = flTimeSpanMS;
    }
}
