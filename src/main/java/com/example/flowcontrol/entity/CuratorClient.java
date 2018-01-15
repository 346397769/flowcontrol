package com.example.flowcontrol.entity;

import com.example.flowcontrol.properties.PublicProperties;
import com.example.flowcontrol.utils.IntLong2BytesUtil;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CuratorClient implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(CuratorClient.class);
    //root路径下的所有节点的数的和
    private static Long zkServerCurrentNumL;

    //要创建的根节点路径
    private static String rootPath = PublicProperties.FL_TEST_NODE_PATH;

    //创建的属于自己的目录
    private static String myPath;

    //当前root目录下的所有节点的路径信息  这里的路径不包含root的路径前缀
    private static List<String> kidsPathUnderRoot;

    //每有一次访问，队列加1，每过一秒钟设置为0
    private static Queue<Long> queue = new ConcurrentLinkedQueue<Long>();

    //当前保存的自己的访问数量
//    private static Long myNum = 0L;
    AtomicInteger myNum = new AtomicInteger(0);

    //访问申请的开关
    private static AtomicBoolean onOff = new AtomicBoolean(true);

    //要连接的zk的url和端口
    private static String[] connectZkUrlPorts = {"10.124.134.37:2181","10.124.134.38:2181"};
    private static String connectZkUrlPort = PublicProperties.CONNECT_ZK_URL_PORT;

    private static CuratorFramework curatorFramework;

    private static CuratorClient curatorClient = new CuratorClient();

    static {
        try {
            // 连接时间 和重试次数
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 6);
            curatorFramework = CuratorFrameworkFactory.builder().connectString(connectZkUrlPort)
                    .retryPolicy(retryPolicy).connectionTimeoutMs(3000)
                    .build();
            curatorFramework.start();
            curatorFramework.blockUntilConnected();
//        //设置节点的cache
//        TreeCache treeCache = new TreeCache(curatorFramework, "/flCtrlTest");
//        //开始监听
//        treeCache.start();
//        //设置监听器和处理过程
//        treeCache.getListenable().addListener(new TreeCacheListener() {
//            @Override
//            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
//                ChildData data = event.getData();
//                if(data !=null){
//                    switch (event.getType()) {
//                        case NODE_ADDED:
//                            log.info("增加节点 : "+ data.getPath() +"  数据:"+ IntLong2BytesUtil.bytes2Long(data.getData()));
//                            break;
//                        case NODE_REMOVED:
//                            log.info("删除节点 : "+ data.getPath());
//                            break;
//                        case NODE_UPDATED:
//                            log.info("更新节点 : "+ data.getPath() +"  数据:"+ IntLong2BytesUtil.bytes2Long(data.getData()));
//                            break;
//                        case INITIALIZED:
//                            log.info("初始化... : ");
//                            break;
//                        case CONNECTION_LOST:
//                            log.info("连接中断...... ");
//                            break;
//                        case CONNECTION_SUSPENDED:
//                            log.info("连接挂起...... ");
//                            break;
//                        case CONNECTION_RECONNECTED:
//                            log.info("正在尝试重新连接...... ");
//                            break;
//                        default:
//                            break;
//                    }
//                }else{
//                    log.info( "data is null : "+ event.getType());
//                }
//            }
//        });
            //初始化流控节点
            initCurator();
        } catch (InterruptedException e) {
            log.error("阻塞线程等待curatorFramework连接zookeeper出错，"+e.getMessage(),e);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    //私有的构造方法，单例的ZkClient
    private CuratorClient(){}

    //获取单例的ZkClient
    public static CuratorClient getCuratorClient() {
        return curatorClient;
    }

    public static boolean isOnOff() {
        return onOff.get();
    }

    public static void setOn() {
        onOff.set(true);
    }

    public static void setOff() {
        onOff.set(false);
    }

    /**
     * 给当前自己的缓存访问数量+1
     */
    public void addOne2MyNum(){
        myNum.getAndIncrement();
//        if (myNum.get() >= 10){
//            addMyNum2NodeValue();
//            myNum.getAndSet(0);
//            if (getZkServerCurrentNumL() < PublicProperties.MAX_VALUE) {
////                   log.info("从队列里取出的值为1......向"+zkPath+"节点+1");
//                //给自己的暂存访问数+1
//                setOn();
//            } else {
//                //这时候队列里面没有数，或者在固定时间内已经超过最大限制数量，休眠些许时间
//                setOff();
//            }
//        }
    }

    public  Integer getMyNum() {
        return myNum.get();
    }

//    public static void setMyNum(Long myNum) {
//        CuratorClient.myNum = myNum;
//    }

    /**
     * 初始化zookeeper的节点
     * 并设置初始值
     */
    private static void initCurator(){
        try {
            //根节点，创建，并初始化
            if (curatorFramework.checkExists().forPath(rootPath) == null){
                curatorFramework.create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(rootPath,"rootFlTestNodeValue".getBytes());
            }
            //自己的子节点创建，初始化
            //代码运行到这里不用判断root节点是否存在，root节点是肯定存在的
            //并且在调用这个initCurator()函数的时候，除了第一次调用进行初始化之外，一定是对这个临时节点操作时找不到这个节点才会报错，这时候再创建一个节点就可以
//            if (curatorFramework.checkExists().forPath(rootPath) == null){
                Long numL = 0L;
                byte[] numLbytes = IntLong2BytesUtil.long2Bytes(numL);
                myPath = curatorFramework.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(rootPath+"/OSN",numLbytes);
//            }
            //当前存的所有节点的sum的刷新  所有节点的路径的刷新
            initClientValues();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    /**
     * root路径下的节点的路径和所有节点的存的数的和  的初始化
     */
    public static void initClientValues(){
        try {
            //获取所有子节点的路径  这里的路径是不包含上一级的路径的不全路径 例如 全路径是  rootPath/myPath   这里获取的是 myPath的List<String>
            getKidsPathUnderRoot();
            //为了防止操作过程中kidsPathUnderRoot被改动
            List<String> kidsPathes = kidsPathUnderRoot;
            Long numCount = 0L;
            for (String pathes:kidsPathes) {
                numCount = numCount + IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath(rootPath+"/"+pathes));
            }
            zkServerCurrentNumL = numCount;
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    /**
     * 将节点里面存的Long类型的值取出
     * @param path
     * @return
     * @throws Exception
     */
    public Long getNodeValue(String path) throws Exception {
        return IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath(path));
    }


    /**
     * 往zkNode节点+1
     * 将最新的值加1  由于是通过队列操作的，并且只有一个线程操作，所以不会有冲突
     * org.apache.zookeeper.KeeperException$BadVersionException 这个异常表示节点版本号不对
     */
    public void addMyNum2NodeValue(Integer num){
        try {
//            int version = curatorFramework.checkExists().forPath(zkPath).getVersion();
            Long currentNum = IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath(myPath));
//            log.info(zkPath+" 节点加1前值为:"+currentNum+",版本号为:"+version);
            curatorFramework.setData().withVersion(-1).forPath(myPath,IntLong2BytesUtil.long2Bytes(currentNum+num));
//            log.info(zkPath+" 节点加1后值为:"+zkServerCurrentNumL+",版本号为:"+(version+1));
        }catch (KeeperException.BadVersionException e) {
//            log.error("给节点加1时，版本号不对，正在重试......"+e.getMessage(),e);
            log.error(e.getMessage(),e);
            //重新调用自己
//            addOne2NodeValue();
        }catch (KeeperException.NoNodeException e) {
            //这里异常是这个节点可能被删除了，重新建立这个节点
            log.error(myPath+" 节点可能被错误删除，正在重新建立节点......"+e.getMessage(),e);
            initCurator();
            addMyNum2NodeValue(num);
            //重新调用自己
//            addOne2NodeValue();
        }catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    /**
     * 获取所有子节点的路径  这里的路径是不包含上一级的路径的不全路径 例如 全路径是  rootPath/myPath   这里获取的是 myPath的List<String>
     * @return
     */
    public static List<String> getKidsPathUnderRoot() {
        try {
            kidsPathUnderRoot  = curatorFramework.getChildren().forPath(rootPath);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
        return kidsPathUnderRoot;
    }

    public static void setKidsPathUnderRoot(List<String> kidsPathUnderRoot) {
        CuratorClient.kidsPathUnderRoot = kidsPathUnderRoot;
    }

    public void addOne2Queue(){
        queue.add(1L);
    }

    /**
     * 获取root路径下的所有节点的数的和
     * @return
     */
    public static Long getZkServerCurrentNumL() {
        initClientValues();
        return zkServerCurrentNumL;
    }

    public static  void setZkNodeValue0(){
        Long numL = 0L;
        byte[] numLbytes = IntLong2BytesUtil.long2Bytes(numL);
        try {
            curatorFramework.setData().withVersion(-1).forPath(myPath, numLbytes);
        } catch (KeeperException.NoNodeException e) {
            //这里异常是这个节点可能被删除了，重新建立这个节点
            log.error(myPath+" 节点可能被错误删除，正在重新建立节点......"+e.getMessage(),e);
            initCurator();
            setZkNodeValue0();
        }catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    public static void setZkServerCurrentNumL(Long zkServerCurrentNumL) {
        CuratorClient.zkServerCurrentNumL = zkServerCurrentNumL;
    }

    public static String getRootPath() {
        return rootPath;
    }

    public static void setRootPath(String rootPath) {
        CuratorClient.rootPath = rootPath;
    }

//    public static Queue<Long> getQueue() {
//        return queue;
//    }
//
//    public static void setQueue(Queue<Long> queue) {
//        CuratorClient.queue = queue;
//    }


    public static String getMyPath() {
        return myPath;
    }

    public static void setMyPath(String myPath) {
        CuratorClient.myPath = myPath;
    }

    public static String[] getConnectZkUrlPorts() {
        return connectZkUrlPorts;
    }

    public static void setConnectZkUrlPorts(String[] connectZkUrlPorts) {
        CuratorClient.connectZkUrlPorts = connectZkUrlPorts;
    }

    public static String getConnectZkUrlPort() {
        return connectZkUrlPort;
    }

    public static void setConnectZkUrlPort(String connectZkUrlPort) {
        CuratorClient.connectZkUrlPort = connectZkUrlPort;
    }

    public static void setCuratorClient(CuratorFramework curatorFramework) {
        CuratorClient.curatorFramework = curatorFramework;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
//        log.info("队列自处理线程开始执行.........");

        /*
        * 如果myNum>0，将它设置为0，然后往zookeeper的节点里加一次，并判断当前节点下的所有的访问量是否超限
        * 如果超限，就关闭访问开关，如果没有就
        * */
        while (true) {
            try {
                Integer num = myNum.get();
                myNum.set(0);
                if (getZkServerCurrentNumL() < PublicProperties.MAX_VALUE) {
                    setOn();
                    if (num > 0){
                        addMyNum2NodeValue(num);
                    }
                } else {
                    //这时候在固定时间内已经超过最大限制数量，休眠些许时间
                    setOff();
                    Thread.sleep(PublicProperties.QUEUE_NO_VALUE_SLEEP_MS);
                }
            } catch (InterruptedException e) {
                log.error("队列操作-----线程休眠出错 ："+e.getMessage(),e);
            } catch (Exception e) {
                log.error(e.getMessage(),e);
            }
        }
    }
}
