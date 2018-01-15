package com.example.flowcontrol.controller;

import com.example.flowcontrol.entity.CuratorClient;
import com.example.flowcontrol.entity.RspInfo;
import com.example.flowcontrol.entity.TestRsp;
import com.example.flowcontrol.properties.PublicProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Timer;
import java.util.TimerTask;

@RestController
public class FlowTestController {
    private static final Logger log = LoggerFactory.getLogger(FlowTestController.class);
    private static CuratorClient curatorClient;
    private static Timer timer = new Timer();

    /**
     * 这个static块里面的代码是按顺序来的，不能修改顺序，否则会造成错误
     */
    static {
        curatorClient = CuratorClient.getCuratorClient();
        //调用队列的自处理任务
        new Thread(curatorClient).start();
        //设置一个timer每隔一段时间设置zk node的值为0
        timer.schedule(new TimerTask() {
            public void run() {
    //            log.info("超过"+PublicProperties.MAX_INTERVAL_MS+"毫秒 设置节点为0......");
                curatorClient.setZkNodeValue0();
//                log.info(PublicProperties.MAX_INTERVAL_MS+"毫秒时间到，将节点值设置为0成功......");
            }
        }, 5000,PublicProperties.MAX_INTERVAL_MS);
    }

    @RequestMapping(value = "/flowTest")
    public RspInfo test(){
        RspInfo rspInfo = new RspInfo();
        curatorClient.addOne2MyNum();
        if (curatorClient.isOnOff()){
            rspInfo.setDesc("success");
        }else {
            rspInfo.setDesc(PublicProperties.MAX_VALUE+"毫秒内访问超过最大限制,拒绝访问！！！");
            log.error(PublicProperties.MAX_VALUE+"毫秒内访问超过最大限制，拒绝访问！！！");
        }
        rspInfo.setZkPath(curatorClient.getKidsPathUnderRoot());
        rspInfo.setConnectZkUrlPort(curatorClient.getConnectZkUrlPort());
        rspInfo.setMaxNum(PublicProperties.MAX_VALUE);
        rspInfo.setCurrentNum(curatorClient.getZkServerCurrentNumL());
//        log.info(rspInfo.toString());
        return rspInfo;
    }

    @RequestMapping(value = "/add")
    public TestRsp testRsp1(){
        TestRsp testRsp = new TestRsp();
        curatorClient.addOne2MyNum();
        testRsp.setSelfCount(curatorClient.getMyNum());
        return testRsp;
    }

    @RequestMapping(value = "/get")
    public TestRsp testRsp2(){
        TestRsp testRsp = new TestRsp();
        testRsp.setSelfCount(curatorClient.getMyNum());
        return testRsp;
    }
}
