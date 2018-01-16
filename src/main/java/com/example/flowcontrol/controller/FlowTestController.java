package com.example.flowcontrol.controller;

import com.example.flowcontrol.entity.CuratorClient;
import com.example.flowcontrol.entity.RspInfo;
import com.example.flowcontrol.entity.TestRsp;
import com.example.flowcontrol.properties.PublicProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FlowTestController {
    private static final Logger log = LoggerFactory.getLogger(FlowTestController.class);
    private static CuratorClient curatorClient;
    private static boolean zkInit = false;

    static {
        curatorClient = CuratorClient.getCuratorClient();
        zkInit = curatorClient.initConnect(PublicProperties.CONNECT_ZK_URL_PORT);
        if (zkInit){
            //如果连接成功，那么开始流量控制
            curatorClient.flClrStart();
        }
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
//        rspInfo.setZkPath(curatorClient.getKidsPathUnderRoot());
//        rspInfo.setConnectZkUrlPort(curatorClient.getConnectZkUrlPort());
        rspInfo.setMaxNum(PublicProperties.MAX_VALUE);
//        rspInfo.setCurrentNum(curatorClient.getZkServerCurrentNumL());
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
