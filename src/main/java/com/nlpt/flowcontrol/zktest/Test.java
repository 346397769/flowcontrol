package com.nlpt.flowcontrol.zktest;

import com.nlpt.flowcontrol.utils.IntLong2BytesUtil;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.List;

public class Test {
    public static void main(String[] args) {
        RetryPolicy retryPolicy = new RetryForever(3000);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().connectString("10.124.164.110:7001")
                .retryPolicy(retryPolicy).namespace("DEV").connectionTimeoutMs(4000)
                .build();

        curatorFramework.start();
        try {
//            long a = System.currentTimeMillis();
//            long num =  IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath("/esb_svc_173025_S/10.124.164.110_8001_20180712172707398"));
//            long b = System.currentTimeMillis();
//            System.out.println(b-a);
            System.out.println(new String(curatorFramework.getData().forPath("/leader")));
//
//                System.out.println(num);


//            curatorFramework.create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath("/TEST","122333112121212121".getBytes());

//            curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath("/111111");
            curatorFramework.close();



//            List<String> list = curatorFramework.getChildren().forPath("/oparty_svc_91710_153289_D");
//            long numCount = 0L;
//            for (String pathes:list) {
//                //加个异常处理，没有这个节点就不要统计了，去统计下一个
//                //getNodeValue 用这个方法
//                try {
//                    numCount = numCount + IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath("/oparty_svc_91710_153289_D/"+pathes));
//                }catch (Exception e) {
//                    // 这里报异常的可能是，节点断线，节点被定时任务删除，维度节点由于leader异常消失时进行增加而不存在
//                    // 但是这里不需要重建节点，因为在 addMyNum2NodeValue 的时候已经检查重建了
//                    // 报异常是正常情况
////                log.error("获取节点"+pathes+"数据失败",e);
//                }
//            }
//            System.out.println(numCount);

        }  catch (Exception e) {
            e.printStackTrace();
        }



//        while (true){
//            try {
////                long num =  IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath("/oparty_svc#103#103_S/10.191.31.114218920180326102359850"));
////
////                System.out.println(num);
//
//                List<String> list = curatorFramework.getChildren().forPath("oparty_svc_91710_153289_D");
//                long numCount = 0L;
//                for (String pathes:list) {
//                    //加个异常处理，没有这个节点就不要统计了，去统计下一个
//                    //getNodeValue 用这个方法
//                    try {
//                        numCount = numCount + getNodeValue("/oparty_svc_91710_153289_D/"+pathes);
//                    }catch (Exception e) {
//                        // 这里报异常的可能是，节点断线，节点被定时任务删除，维度节点由于leader异常消失时进行增加而不存在
//                        // 但是这里不需要重建节点，因为在 addMyNum2NodeValue 的时候已经检查重建了
//                        // 报异常是正常情况
////                log.error("获取节点"+pathes+"数据失败",e);
//                    }
//                }
//
//            }  catch (Exception e) {
//                e.printStackTrace();
//            }
//        }

//        curatorFramework.close();
    }
}
