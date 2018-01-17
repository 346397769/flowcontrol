package com.example.flowcontrol.entity;


import java.util.Arrays;
import java.util.List;

public class RspInfo {
    private Long SumNum;
    private Integer MyNum;
    private Long maxNum;
    private String desc;
    private String myNodePath;
    private List<String> uderRootPathes;
    private String myConnectPath;

    @Override
    public String toString() {
        return "RspInfo{" +
                "SumNum=" + SumNum +
                ", MyNum=" + MyNum +
                ", maxNum=" + maxNum +
                ", desc='" + desc + '\'' +
                ", myNodePath='" + myNodePath + '\'' +
                ", uderRootPathes=" + uderRootPathes +
                ", myConnectPath='" + myConnectPath + '\'' +
                '}';
    }

    public List<String> getUderRootPathes() {
        return uderRootPathes;
    }

    public void setUderRootPathes(List<String> uderRootPathes) {
        this.uderRootPathes = uderRootPathes;
    }

    public Long getSumNum() {
        return SumNum;
    }

    public void setSumNum(Long sumNum) {
        SumNum = sumNum;
    }

    public Integer getMyNum() {
        return MyNum;
    }

    public void setMyNum(Integer myNum) {
        MyNum = myNum;
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

    public String getMyNodePath() {
        return myNodePath;
    }

    public void setMyNodePath(String myNodePath) {
        this.myNodePath = myNodePath;
    }


    public String getMyConnectPath() {
        return myConnectPath;
    }

    public void setMyConnectPath(String myConnectPath) {
        this.myConnectPath = myConnectPath;
    }
}
