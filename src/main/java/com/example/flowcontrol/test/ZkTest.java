package com.example.flowcontrol.test;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ZkTest implements Watcher{
    private static Logger log = LoggerFactory.getLogger(ZkTest.class);

    private static final String connectZkUrl = "10.124.134.37:2181";

    private static final String zkPath = "/tokenNum";

    private static  String controlNum = "500";

    private static ZooKeeper zooKeeper;

    public ZkTest(){
        //进行一些初始化工作
        //建立连接
        try {
            zooKeeper = new ZooKeeper(connectZkUrl,3000,this);
            //判断是否连接成功，因为创建连接需要大约两秒，所以要判断连接上了才能往下走
            while (zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
                Thread.sleep(3000);
            }
            //如果没有这个流控的节点的话，创建，并初始化
            if(zooKeeper.exists(zkPath, true)==null){
                zooKeeper.create(zkPath, controlNum.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            log.error("ZookeeperClient 初始化连接失败 : "+e.getMessage(),e);
        } catch (InterruptedException e) {
            log.error("在进行睡眠暂停连接zk的线程时出错 : "+e.getMessage(),e);
        } catch (KeeperException e) {
            log.error("Zookeeper创建节点失败 : "+e.getMessage(),e);
        }
    }

    /**
     * 更新节点的值，并且返回更新后的值
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    private String updateNumValue() throws KeeperException, InterruptedException {

        return zooKeeper.getData(zkPath,true,null).toString();
    }

    public static String getConnectZkUrl() {
        return connectZkUrl;
    }

    public static String getZkPath() {
        return zkPath;
    }

    public static String getControlNum() {
        return controlNum;
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.None) {
            log.info("连接服务器成功!");
        } else if (event.getType() == Watcher.Event.EventType.NodeCreated) {
            log.info("节点创建成功!");
        } else if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            log.info("子节点创建更新成功!");
            //读取新的配置
        } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
            log.info("节点更新成功!");
            //读取新的配置
        } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
            log.info("节点删除成功!");
        }
    }

    public static void main(String[] args) {
//        try {
//            ZkTest zkTest = new ZkTest();
//            zkTest.getZooKeeper().close();
//        } catch (InterruptedException e) {
//            log.error("关闭zookeeper出错 : "+e.getMessage());
//        }
        Queue<Integer> queue=new ConcurrentLinkedQueue<Integer>();
//        for(int i = 0; i < 1000; i++){
//            queue.add(i);
//        }
//        while (!queue.isEmpty()){
//            System.out.print(queue.poll()+" ");
//        }
        System.out.println(queue.peek());
    }
}
