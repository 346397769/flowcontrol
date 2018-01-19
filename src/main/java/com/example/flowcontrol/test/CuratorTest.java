package com.example.flowcontrol.test;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CuratorTest {
    public static void main(String[] args) throws Exception {
        //zk 地址
        String connectString = "10.124.134.37:2181";
        // 连接时间 和重试次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 3);

//        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(connectString)
                .retryPolicy(retryPolicy).namespace("syf").connectionTimeoutMs(3000)
                .build();


        client.start();


        client.blockUntilConnected();

        ExecutorService pool = Executors.newCachedThreadPool();

        //设置节点的cache
        TreeCache treeCache = new TreeCache(client, "/");
                //开始监听
        treeCache.start();
        //设置监听器和处理过程
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                if(data !=null){
                    switch (event.getType()) {
                        case NODE_ADDED:
                            System.out.println("NODE_ADDED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                            break;
                        case NODE_REMOVED:
                            System.out.println("NODE_REMOVED : "+ data.getPath());
                            break;
                        case NODE_UPDATED:
                            System.out.println("NODE_UPDATED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                            break;

                        default:
                            break;
                    }
                }else{
                    System.out.println( "data is null : "+ event.getType());
                }
            }
        });


        String myPath = "/OSN";
        System.out.println(myPath);
        if (client.checkExists().forPath("/CuratorTest") == null){
            client.create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath("/CuratorTest","hello".getBytes());
        }
        if (client.checkExists().forPath("/CuratorTest1") == null){
            client.create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath("/CuratorTest1","hello".getBytes());
        }
//        if (client.checkExists().forPath("/test1"+myPath) == null){
//            myPath = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath("/CuratorTest"+myPath,"hello".getBytes());
//            System.out.println("myPath : "+myPath);
//        }
        List<String> list = client.getChildren().forPath("/");
        System.out.println(list);


        treeCache.close();
        client.close();
    }
}
