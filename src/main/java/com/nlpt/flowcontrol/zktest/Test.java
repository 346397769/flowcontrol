package com.nlpt.flowcontrol.zktest;

import com.nlpt.flowcontrol.utils.IntLong2BytesUtil;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

public class Test {
    public static void main(String[] args) {
        RetryPolicy retryPolicy = new RetryForever(3000);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().connectString("10.124.134.37:2181")
                .retryPolicy(retryPolicy).namespace("BASE").connectionTimeoutMs(4000)
                .build();

        curatorFramework.start();


        while (true){
            try {
//                long num =  IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath("/oparty_svc#103#103_S/10.191.31.114218920180326102359850"));
//
//                System.out.println(num);
                if (curatorFramework.checkExists().forPath("/leaderSelectSuccess") != null){
                    System.out.println("存在");
                }else {
                    System.out.println("消失了");
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

//        curatorFramework.close();
    }
}
