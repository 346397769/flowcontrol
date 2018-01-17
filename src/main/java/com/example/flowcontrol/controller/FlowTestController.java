package com.example.flowcontrol.controller;

import com.example.flowcontrol.entity.CuratorClient;
import com.example.flowcontrol.entity.RspInfo;
import com.example.flowcontrol.properties.PublicProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
        CuratorClient.addOne2MyNum();
        if (CuratorClient.isOnOff() && CuratorClient.getConnectToServer()){
            rspInfo.setDesc("successWithFL");
            rspInfo.setMyNum(CuratorClient.getMyNum());
            rspInfo.setMyConnectPath(CuratorClient.getCurrentConnectString());
            rspInfo.setMyNodePath(CuratorClient.getMyPath());
            rspInfo.setUderRootPathes(CuratorClient.getKidsPathUnderRootOut());
            rspInfo.setSumNum(CuratorClient.getZkServerCurrentNumLOut());
        }else if(CuratorClient.isOnOff() && CuratorClient.getConnectToServer() == false){
            //此时是没有连接到服务器，并且开关是打开的
            rspInfo.setDesc("successWithoutFL");
        } else if (CuratorClient.isOnOff() == false && CuratorClient.getConnectToServer()){
            //此时是连接到服务器，并且开关关闭
            rspInfo.setDesc(PublicProperties.MAX_VALUE+"毫秒内访问超过最大限制,拒绝访问！！！");
            log.info(PublicProperties.MAX_VALUE+"毫秒内访问超过最大限制，拒绝访问！！！");
        }
        rspInfo.setMaxNum(PublicProperties.MAX_VALUE);
        return rspInfo;
    }
}
