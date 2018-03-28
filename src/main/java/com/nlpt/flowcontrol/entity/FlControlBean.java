package com.nlpt.flowcontrol.entity;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FlControlBean {
    //维度
    private String dimension;
    //缓存本地的当前请求次数
    private AtomicInteger myNum = new AtomicInteger(0);
    //固定时间内可以请求的次数
    private int maxVisitValue;
    //流控固定时间长度 毫秒
    private int flTimeSpanMS;
    // 当前自己的叶子节点的全路径
    private String myPath;
    //访问申请的开关
    private AtomicBoolean onOff = new AtomicBoolean(true);

    //myPath临时节点的路径，在传入CuratorClient的时候会自动生成，所以不用传入
    public FlControlBean(String dimension, int maxVisitValue, int flTimeSpanMS) {
        this.dimension = dimension;
        this.maxVisitValue = maxVisitValue;
        this.flTimeSpanMS = flTimeSpanMS;
        this.myPath = "";
    }

    public FlControlBean() {
    }

    public boolean getOnOff() {
        return onOff.get();
    }

//    public void setOnOff(boolean onOff) {
//        this.onOff.set(onOff);
//    }

    public void setOn() {
        this.onOff.set(true);
    }

    public void setOff() {
        this.onOff.set(false);
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public int getMyNum() {
        return myNum.get();
    }

    public void addOne2MyNum() {
        this.myNum.getAndIncrement();
    }

    public void decreaseMyNum(int num){
        myNum.addAndGet(-num);
    }

    public void setMyNum(Integer myNum) {
        this.myNum.set(myNum);
    }

    public int getMaxVisitValue() {
        return maxVisitValue;
    }

    public void setMaxVisitValue(Integer maxVisitValue) {
        this.maxVisitValue = maxVisitValue;
    }

    public int getFlTimeSpanMS() {
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
