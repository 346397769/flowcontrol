package com.example.flowcontrol.entity;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FlControlBean {
    //维度
    private String dimension;
    //缓存本地的当前请求次数
    private AtomicInteger myNum = new AtomicInteger(0);
    //固定时间内可以请求的次数
    private Integer maxVisitValue;
    //流控固定时间长度 毫秒
    private Integer flTimeSpanMS;
    // 当前自己的叶子节点的全路径
    private String myPath;
    //访问申请的开关
    private AtomicBoolean onOff = new AtomicBoolean(true);
    //上一次重置自己临时的节点为0的毫秒数
    private Long lastTimeSet02MyTempZkNode = 0L;

    //myPath临时节点的路径，在传入CuratorClient的时候会自动生成，所以不用传入
    public FlControlBean(String dimension, Integer maxVisitValue, Integer flTimeSpanMS) {
        this.dimension = dimension;
        this.maxVisitValue = maxVisitValue;
        this.flTimeSpanMS = flTimeSpanMS;
        this.myPath = "";
    }

    public FlControlBean() {
    }

    @Override
    public String toString() {
        return "FlControlBean{" +
                "dimension='" + dimension + '\'' +
                ", myNum=" + myNum +
                ", maxVisitValue=" + maxVisitValue +
                ", flTimeSpanMS=" + flTimeSpanMS +
                ", myPath='" + myPath + '\'' +
                ", onOff=" + onOff +
                ", lastTimeSet02MyTempZkNode=" + lastTimeSet02MyTempZkNode +
                '}';
    }

    public Long getLastTimeSet02MyTempZkNode() {
        return lastTimeSet02MyTempZkNode;
    }

    /**
     * 设置 上一次重置自己临时的节点为0的毫秒数 为当前时间毫秒数
     * @param
     */
    public void setLastTimeSet02MyTempZkNode() {
        this.lastTimeSet02MyTempZkNode = new Date().getTime();
    }

    public boolean getOnOff() {
        return onOff.get();
    }

    public void setOnOff(boolean onOff) {
        this.onOff.set(onOff);
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public Integer getMyNum() {
        return myNum.get();
    }

    public void addOne2MyNum() {
        this.myNum.getAndIncrement();
    }

    public void setMyNum(Integer myNum) {
        this.myNum.set(myNum);
    }

    public Integer getMaxVisitValue() {
        return maxVisitValue;
    }

    public void setMaxVisitValue(Integer maxVisitValue) {
        this.maxVisitValue = maxVisitValue;
    }

    public Integer getFlTimeSpanMS() {
        return flTimeSpanMS;
    }

    public void setFlTimeSpanMS(Integer flTimeSpanMS) {
        this.flTimeSpanMS = flTimeSpanMS;
    }

    public String getMyPath() {
        return myPath;
    }

    public void setMyPath(String myPath) {
        this.myPath = myPath;
    }
}
