package com.example.flowcontrol.controller;

import com.example.flowcontrol.entity.CuratorClient;
import com.example.flowcontrol.entity.FlControlBean;
import com.example.flowcontrol.entity.FlStatus;
import com.example.flowcontrol.entity.RspInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
        list.add(new FlControlBean("AOP",500,2000));
        list.add(new FlControlBean("CBSS",400,1000));
        curatorClient.initFl(list);
    }

    @RequestMapping(value = "/flowTest")
    public RspInfo test(){
        RspInfo rspInfo = new RspInfo();
        if (curatorClient.doFlowControl("AOP") == FlStatus.OK){
            rspInfo.setDesc("successWithFl");
        }else if (curatorClient.doFlowControl("AOP") == FlStatus.NO){
            rspInfo.setDesc("successWithoutFl");
        }else {
            rspInfo.setDesc("lostConnect");
        }
        return rspInfo;
    }
}
