package com.nlpt.flowcontrol.entity;

import java.util.Timer;
import java.util.TimerTask;

public class SettingZero{

    private CuratorClient curatorClient;

    private FlControlBean flControlBean;

    private Timer timer = new Timer();


    public SettingZero(FlControlBean flControl,CuratorClient cuClient){
        curatorClient = cuClient;
        flControlBean = flControl;
    }

    public void startSetting0TimerTask(){
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                curatorClient.deleteDimensionNodes(flControlBean);
            }
        },1000,flControlBean.getFlTimeSpanMS());
    }

    public void cancelTimerTask(){
        timer.cancel();
    }
}
