package com.nlpt.flowcontrol.utils;

import com.nlpt.flowcontrol.entity.CuratorClient;
import com.nlpt.flowcontrol.entity.FlStatus;
import com.nlpt.flowcontrol.entity.FlControlBean;
import org.apache.curator.utils.CloseableUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class FlowControlUtil {
    private static CuratorClient curatorClient;

    private static AtomicBoolean initSuccess = new AtomicBoolean(true);

    static {
        String dateString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
        curatorClient = new CuratorClient("BASE","10.124.134.38:2181",dateString);
        if (!curatorClient.initConnect()){
            curatorClient.stopCurator();
            initSuccess.set(false);
        }
    }

    public static AtomicBoolean getInitSuccess() {
        return initSuccess;
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

    public static void  stopCurator(){
        curatorClient.stopCurator();
    }

    public static void  startCurator(){
        curatorClient.startCurator();
    }
}
