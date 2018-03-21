package com.nlpt.flowcontrol.controller;

import com.nlpt.flowcontrol.entity.CuratorClient;
import com.nlpt.flowcontrol.entity.FlStatus;
import com.nlpt.flowcontrol.entity.RspInfo;
import com.nlpt.flowcontrol.entity.FlControlBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class FlowTestController {
    private static final Logger log = LoggerFactory.getLogger(FlowTestController.class);
    private static CuratorClient curatorClient;

    static {
        curatorClient = new CuratorClient("BASE","10.124.134.37:2181,10.124.134.38:2181,10.124.134.39:2181,10.124.128.195:2181,10.124.128.196:2181");
        List<FlControlBean> list = new ArrayList<FlControlBean>();
//        list.add(new FlControlBean("AOP",500,1000));
        list.add(new FlControlBean("CBSS",1000,1000));
//        list.add(new FlControlBean("TEST",1000,1000*60*60));
        curatorClient.initConnect();
        curatorClient.initFl(list);
    }

    @RequestMapping(value = "/flowTest/{dimension}")
    public RspInfo test(@PathVariable("dimension") String dimension){
        RspInfo rspInfo = new RspInfo();
        rspInfo.setDimension(dimension);
        FlStatus flResult = curatorClient.doFlowControl(dimension);
        if (flResult == FlStatus.OK){
            rspInfo.setDesc("successWithFl");
        }else if (flResult == FlStatus.NO){
            rspInfo.setDesc("successWithoutFl");
        }else {
            rspInfo.setDesc("lostConnect or no this dimension");
        }
        return rspInfo;
    }
}
