package com.nlpt.flowcontrol.utils;

import com.nlpt.flowcontrol.entity.CuratorClient;
import com.nlpt.flowcontrol.entity.FlStatus;
import com.nlpt.flowcontrol.entity.FlControlBean;
import com.nlpt.flowcontrol.entity.ZkServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class FlowControlUtil {
    private static final Log log = LogFactory.getLog(FlowControlUtil.class);

    private static ZkServer zkServer;

    private static CuratorClient curatorClient;

    private static AtomicBoolean initSuccess = new AtomicBoolean(true);

    static {
//        Properties properties = new Properties();
//        //初始化properties参数
//        InputStream zooInput = ZkServer.class.getResourceAsStream("/conf/zoo.cfg");
//        try {
//            properties.load(zooInput);
//            zooInput.close();
//        } catch (IOException e) {
//            log.error(e.getMessage(),e);
//        }
//
//        zkServer = new ZkServer(properties);


        String dateString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
        curatorClient = new CuratorClient("EMBED","10.124.164.110:7001",dateString);
//        zkServer.startClusterZkServer();
//        try {
//            // 等待zk服务端互连
//            Thread.sleep(15000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        if (curatorClient.initConnect()){
            log.info("zookeeper客户端启动成功 啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊");
        }


//            if (zkServer.getZkServerStartStatus()){
//                log.info("zookeeper服务端启动成功");
//                if (!curatorClient.initConnect()){
//                    initSuccess.set(false);
//                }else {
//                    log.info("zookeeper客户端启动成功");
//                }
//            }
//            else {
//                log.error("zookeeper服务端启动失败");
//                curatorClient.stopCurator();
//                zkServer.stopZkServer();
//            }

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
        curatorClient.initConnect();
    }
}
