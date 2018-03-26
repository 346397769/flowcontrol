package com.nlpt.flowcontrol.zktest;

import com.nlpt.flowcontrol.utils.IntLong2BytesUtil;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

public class Test {
    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new RetryForever(3000);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().connectString("10.191.31.115:2181")
                .retryPolicy(retryPolicy).namespace("BASE").connectionTimeoutMs(4000)
                .build();

        curatorFramework.start();

        long num =  IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath("/oparty_svc#103#115_S/5231045dd1d546e0a571ef36b19582cd"));

        System.out.println(num);

        curatorFramework.close();
    }
}
