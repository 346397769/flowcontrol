package com.example.flowcontrol.entity;

public class RspInfo {
    private String desc;
    private String dimension;

    @Override
    public String toString() {
        return "RspInfo{" +
                "desc='" + desc + '\'' +
                ", dimension='" + dimension + '\'' +
                '}';
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
