package com.nlpt.flowcontrol.utils;

import com.nlpt.flowcontrol.entity.CuratorClient;
import com.nlpt.flowcontrol.entity.FlStatus;
import com.nlpt.flowcontrol.entity.FlControlBean;

import java.util.List;

public class FlowControlUtil {
    private static CuratorClient curatorClient;

    static {
        curatorClient = new CuratorClient("BASE","10.124.134.37:2181,10.124.134.38:2181,10.124.134.39:2181,10.124.128.195:2181,10.124.128.196:2181");
    }

    /**
     * 将需要初始化的节点传入，此操作需要在同步内存库的时候去做
     * @param flControlBeans
     */
    public static void initFlNodes(List<FlControlBean> flControlBeans){
        curatorClient.initFl(flControlBeans);
    }

    /**
     * 每次调用服务的时候去执行这个操作，根据返回值判断此时还有没有流量
     * 返回值
     * @param dimension
     * @return
     */
    public static FlStatus doFlowControl(String dimension){
        return curatorClient.doFlowControl(dimension);
    }
}
