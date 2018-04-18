package com.nlpt.flowcontrol.controller;

import com.nlpt.flowcontrol.entity.CuratorClient;
import com.nlpt.flowcontrol.entity.FlStatus;
import com.nlpt.flowcontrol.entity.RspInfo;
import com.nlpt.flowcontrol.entity.FlControlBean;
import com.nlpt.flowcontrol.utils.FlowControlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RestController
public class FlowTestController {
    private static final Logger log = LoggerFactory.getLogger(FlowTestController.class);
//    private static CuratorClient curatorClient;
//
//    static {
//        String dateString = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
//        curatorClient = new CuratorClient("BASE","10.124.134.37:2181",dateString,false);
//        List<FlControlBean> list = new ArrayList<FlControlBean>();
////        list.add(new FlControlBean("AOP",500,1000));
//        list.add(new FlControlBean("CBSS",1000,1000));
//        list.add(new FlControlBean("TEST",1000,1000*60*60));
//        curatorClient.initConnect();
//        curatorClient.initFl(list);
//    }

    @RequestMapping(value = "/flowTest/{dimension}")
    public RspInfo test(@PathVariable("dimension") String dimension){
        RspInfo rspInfo = new RspInfo();
        rspInfo.setDimension(dimension);
        FlStatus flResult = FlowControlUtil.doFlowControl(dimension);
        if (flResult == FlStatus.OK){
            rspInfo.setDesc("successWithFl");
        }else if (flResult == FlStatus.NO){
            rspInfo.setDesc("successWithoutFl");
        }else {
            rspInfo.setDesc("lostConnect or no this dimension");
        }
        return rspInfo;
    }

    @RequestMapping(value = "/stop")
    public RspInfo stop(){
        RspInfo rspInfo = new RspInfo();
        rspInfo.setDesc("成功关闭客户端");
        FlowControlUtil.stopCurator();
        return rspInfo;
    }

    @RequestMapping(value = "/start")
    public RspInfo start(){
        RspInfo rspInfo = new RspInfo();
        rspInfo.setDesc("成功打开客户端");
        FlowControlUtil.startCurator();
        List<FlControlBean> list = new ArrayList<FlControlBean>();
//        list.add(new FlControlBean("AOP",500,1000));
        list.add(new FlControlBean("CBSS",1000,1000));
        list.add(new FlControlBean("TEST",1000,1000*60*60));
        FlowControlUtil.initFlNodes(list);
        return rspInfo;
    }
}
