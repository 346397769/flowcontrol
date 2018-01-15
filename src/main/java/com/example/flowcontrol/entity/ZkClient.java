package com.example.flowcontrol.entity;

import com.example.flowcontrol.utils.IntLong2BytesUtil;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ZkClient implements Watcher{
    private static final Logger log = LoggerFactory.getLogger(ZkClient.class);

    //当前的服务端自己的节点存储的值 随着每次更新zk服务器端值而更新
    private static Long zkServerCurrentNumL;

    private static String zkPath = "/flowControl";

    //每有一次访问，队列加1，每过一秒钟设置为0
    private static Queue<Long> queue = new ConcurrentLinkedQueue<Long>();

    //要连接的zk的url和端口
    private static String[] connectZkUrlPorts = {"10.124.134.37:2181","10.124.134.38:2181"};
    private static String connectZkUrlPort = "10.124.134.37:2181";

    //ZooKeeper客户端类 用来操作服务端
    private static ZooKeeper zooKeeper;

    //建立一个单例的zkClient
    private static ZkClient zkClient = new ZkClient();

    //私有的构造方法，单例的ZkClient
    private ZkClient(){
        initZk(1);
    }

    //获取单例的ZkClient
    public static ZkClient getZkClient(String connectZkUrlPort) {
        return zkClient;
    }

    /**
     * 初始化zookeeper
     * 连接服务器并且创建节点
     * 并设置初始值
     */
    private  boolean initZk(int retryNum){
        try {
            if (retryNum >= 3){
                return false;
            }
            zooKeeper = new ZooKeeper(connectZkUrlPort, 3000, this);

            //判断是否连接成功，因为创建连接需要大约两秒，所以要判断连接上了才能往下走
            while (zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
                Thread.sleep(3000);
            }
            //如果没有这个流控的节点的话，创建，并初始化
            if(zooKeeper.exists(zkPath, true)==null){
                Long numL = 0L;
                byte[] numLbytes = IntLong2BytesUtil.long2Bytes(numL);
                zooKeeper.create(zkPath, numLbytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }
            return true;
        } catch (IOException e) {
            log.error("ZookeeperClient 初始化连接失败 : "+e.getMessage(),e);
        } catch (InterruptedException e) {
            log.error("在进行睡眠暂停连接zk的线程时出错 : "+e.getMessage(),e);
        } catch (KeeperException e) {
            log.error("Zookeeper创建节点失败 : "+e.getMessage(),e);
        }
        return false;
    }



//    public String createZkNode(String path, byte[] bytes, List<ACL> acl,CreateMode createMode) throws KeeperException, InterruptedException {
//        return zooKeeper.create(path, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//    }


    /**
     * 获取节点的值
     * @param path 路径
     * @param watcher true 设置默认的监听 false不设置监听
     * @param stat 节点状态
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Long getZkNodeValue(String path, boolean watcher, Stat stat) throws KeeperException, InterruptedException {
        byte[] bytesValue = zooKeeper.getData(path,watcher,stat);
        Long numL = IntLong2BytesUtil.bytes2Long(bytesValue);
        return numL;
    }

    /**
     * 设置节点的值
     * @param path 节点的路径
     * @param numL 设置的值 Long类型
     * @param version 版本号
     */
    public void setDataValue(String path, Long numL, int version) throws KeeperException, InterruptedException {
        byte[] bytesNumL = IntLong2BytesUtil.long2Bytes(numL);
        zooKeeper.setData(path,bytesNumL,version);
    }

    public Long getZkServerCurrentNumL() {
        return zkServerCurrentNumL;
    }

    public void setZkServerCurrentNumL(Long zkServerCurrentNumL) {
        this.zkServerCurrentNumL = zkServerCurrentNumL;
    }

    public static String getZkPath() {
        return zkPath;
    }

    public static void setZkPath(String zkPath) {
        ZkClient.zkPath = zkPath;
    }

    public static Queue<Long> getQueue() {
        return queue;
    }

    public static void setQueue(Queue<Long> queue) {
        ZkClient.queue = queue;
    }

    public String getConnectZkUrlPort() {
        return connectZkUrlPort;
    }

    public void setConnectZkUrlPort(String connectZkUrlPort) {
        this.connectZkUrlPort = connectZkUrlPort;
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public static ZkClient getZkClient() {
        return zkClient;
    }

    public static void setZkClient(ZkClient zkClient) {
        ZkClient.zkClient = zkClient;
    }


    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Watcher.Event.EventType.None) {
            if (event.getState() == Event.KeeperState.SyncConnected){
                log.info("连接服务器"+connectZkUrlPort+"成功!");
            } else if (event.getState() == Event.KeeperState.Expired || event.getState() == Event.KeeperState.Disconnected){
                //断开连接后自动重新连接 3次
                for (int i = 1 ; i<=3; i++){
                    if (initZk(i)){
                        break;
                    }
                }
            }
        } else if (event.getType() == Watcher.Event.EventType.NodeCreated) {
            log.info("节点创建成功!");
        } else if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            log.info("子节点创建更新成功!");
            //读取新的配置
        } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
            log.info("节点更新成功!");
            //读取新的配置
            try {
                zkServerCurrentNumL = getZkNodeValue(zkPath,true,null);
            } catch (KeeperException e) {
                log.error(e.getMessage(),e);
            } catch (InterruptedException e) {
                log.error(e.getMessage(),e);
            }
        } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
            log.info("节点删除成功!");
        }
    }
}
