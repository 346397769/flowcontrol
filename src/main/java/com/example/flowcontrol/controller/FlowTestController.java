package com.example.flowcontrol.controller;

import com.example.flowcontrol.entity.CuratorClient;
import com.example.flowcontrol.entity.FlControlBean;
import com.example.flowcontrol.entity.FlStatus;
import com.example.flowcontrol.entity.RspInfo;
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
        curatorClient = CuratorClient.getCuratorClient();
        List<FlControlBean> list = new ArrayList<FlControlBean>();
        list.add(new FlControlBean("AOP",500,1000));
        list.add(new FlControlBean("CBSS",1000,1000));
        curatorClient.initFl(list);
    }

    @RequestMapping(value = "/flowTest/{dimension}")
    public RspInfo test(@PathVariable("dimension") String dimension){
        RspInfo rspInfo = new RspInfo();
        rspInfo.setDimension(dimension);
        FlStatus flResult = curatorClient.doFlowControl(dimension);
        if (flResult == FlStatus.OK || flResult == FlStatus.WRONG_DIMENSION){
            rspInfo.setDesc("successWithFl");
        }else if (flResult == FlStatus.NO){
            rspInfo.setDesc("successWithoutFl");
        }else {
            rspInfo.setDesc("lostConnect");
        }
        return rspInfo;
    }
}
