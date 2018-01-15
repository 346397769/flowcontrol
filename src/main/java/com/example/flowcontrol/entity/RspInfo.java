package com.example.flowcontrol.entity;


import java.util.List;

public class RspInfo {
    private Long currentNum;
    private String connectZkUrlPort;
    private Long maxNum;
    private String desc;
    private List<String> zkPath;

    @Override
    public String toString() {
        return "RspInfo{" +
                "currentNum=" + currentNum +
                ", connectZkUrlPort='" + connectZkUrlPort + '\'' +
                ", maxNum=" + maxNum +
                ", desc='" + desc + '\'' +
                ", zkPath=" + zkPath +
                '}';
    }

    public Long getCurrentNum() {
        return currentNum;
    }

    public void setCurrentNum(Long currentNum) {
        this.currentNum = currentNum;
    }

    public String getConnectZkUrlPort() {
        return connectZkUrlPort;
    }

    public void setConnectZkUrlPort(String connectZkUrlPort) {
        this.connectZkUrlPort = connectZkUrlPort;
    }

    public Long getMaxNum() {
        return maxNum;
    }

    public void setMaxNum(Long maxNum) {
        this.maxNum = maxNum;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public List<String> getZkPath() {
        return zkPath;
    }

    public void setZkPath(List<String> zkPath) {
        this.zkPath = zkPath;
    }
}
