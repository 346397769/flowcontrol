package com.example.flowcontrol.entity;

import com.example.flowcontrol.properties.PublicProperties;
import com.example.flowcontrol.utils.IntLong2BytesUtil;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CuratorClient{
    private static final Logger log = LoggerFactory.getLogger(CuratorClient.class);
    //root路径下的所有节点的数的和
    private static Long zkServerCurrentNumL;

    //要创建的根节点路径
    private static String rootPath = PublicProperties.FL_TEST_NODE_PATH;

    //固定时间内可以请求的次数
    private static Long maxVisitValue = 500L;

    //流控固定时间长度 毫秒
    private static Integer flTimeSpanMS = 1000;

    //超过限制的时候，休眠的时间 毫秒
    private static  Integer overLimitSleepMS = 100;

    //创建的属于自己的目录
    private static String myPath;

    //当前root目录下的所有节点的路径信息  这里的路径不包含root的路径前缀
    private static List<String> kidsPathUnderRoot;

    //当前保存的自己的访问数量
//    private static Long myNum = 0L;
    private static AtomicInteger myNum = new AtomicInteger(0);

    //访问申请的开关
    private static  AtomicBoolean onOff = new AtomicBoolean(true);

    //流控客户端是否连接到服务器
    private static AtomicBoolean connectToServer = new AtomicBoolean(true);

    //要连接的zk的url和端口
    private  String connectZkUrlPort;

    //curator的客户端
    private static CuratorFramework curatorFramework;

    private static CuratorClient curatorClient = new CuratorClient();

    //当前连接的地址
    private static String currentConnectString;

//    static {
//        try {
//            // 连接时间 和重试次数
//            RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 6);
//            curatorFramework = CuratorFrameworkFactory.builder().connectString(connectZkUrlPort)
//                    .retryPolicy(retryPolicy).connectionTimeoutMs(3000)
//                    .build();
//            curatorFramework.start();
//            curatorFramework.blockUntilConnected();
////        //设置节点的cache
////        TreeCache treeCache = new TreeCache(curatorFramework, "/flCtrlTest");
////        //开始监听
////        treeCache.start();
////        //设置监听器和处理过程
////        treeCache.getListenable().addListener(new TreeCacheListener() {
////            @Override
////            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
////                ChildData data = event.getData();
////                if(data !=null){
////                    switch (event.getType()) {
////                        case NODE_ADDED:
////                            log.info("增加节点 : "+ data.getPath() +"  数据:"+ IntLong2BytesUtil.bytes2Long(data.getData()));
////                            break;
////                        case NODE_REMOVED:
////                            log.info("删除节点 : "+ data.getPath());
////                            break;
////                        case NODE_UPDATED:
////                            log.info("更新节点 : "+ data.getPath() +"  数据:"+ IntLong2BytesUtil.bytes2Long(data.getData()));
////                            break;
////                        case INITIALIZED:
////                            log.info("初始化... : ");
////                            break;
////                        case CONNECTION_LOST:
////                            log.info("连接中断...... ");
////                            break;
////                        case CONNECTION_SUSPENDED:
////                            log.info("连接挂起...... ");
////                            break;
////                        case CONNECTION_RECONNECTED:
////                            log.info("正在尝试重新连接...... ");
////                            break;
////                        default:
////                            break;
////                    }
////                }else{
////                    log.info( "data is null : "+ event.getType());
////                }
////            }
////        });
//            //初始化流控节点
//            initCuratorNodes();
//        } catch (InterruptedException e) {
//            log.error("阻塞线程等待curatorFramework连接zookeeper出错，"+e.getMessage(),e);
//        } catch (Exception e) {
//            log.error(e.getMessage(),e);
//        }
//    }


    public static Integer getOverLimitSleepMS() {
        return overLimitSleepMS;
    }

    public static void setOverLimitSleepMS(Integer overLimitSleepMS) {
        CuratorClient.overLimitSleepMS = overLimitSleepMS;
    }

    public static Integer getFlTimeSpanMS() {
        return flTimeSpanMS;
    }

    public static void setFlTimeSpanMS(Integer flTimeSpanMS) {
        CuratorClient.flTimeSpanMS = flTimeSpanMS;
    }

    public static Long getMaxVisitValue() {
        return maxVisitValue;
    }

    public static void setMaxVisitValue(Long maxVisitValue) {
        CuratorClient.maxVisitValue = maxVisitValue;
    }

    public static String getCurrentConnectString() {
        return currentConnectString;
    }

    public static void setCurrentConnectString(String currentConnectString) {
        CuratorClient.currentConnectString = currentConnectString;
    }

    public static boolean getConnectToServer(){
        return connectToServer.get();
    }

    //私有的构造方法，单例的ZkClient
    private CuratorClient(){
    }

    //获取单例的ZkClient
    public static CuratorClient getCuratorClient() {
        return curatorClient;
    }

    public static  boolean isOnOff() {
        return onOff.get();
    }

    public  void setOn() {
        onOff.set(true);
    }

    public  void setOff() {
        onOff.set(false);
    }

    /**
     * 给当前自己的缓存访问数量+1
     */
    public static void addOne2MyNum(){
        myNum.getAndIncrement();
    }

    public static Integer getMyNum() {
        return myNum.get();
    }

    public  void setMyNum(Integer num) {
        myNum.set(num);
    }

    /**
     * 初始化zookeeper的节点
     * 并设置初始值
     */
    private void initCuratorNodes(){
        try {
            //根节点，创建，并初始化
            if (curatorFramework.checkExists().forPath(rootPath) == null){
                curatorFramework.create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(rootPath,"rootFlTestNodeValue".getBytes());
            }
            //自己的子节点创建，初始化
            //首先判断之前是否有，有的话删除
            if (myPath !=null && !myPath.equals("") && curatorFramework.checkExists().forPath(myPath) != null){
                curatorFramework.delete().forPath(myPath);
                log.info("删除节点:"+myPath);
            }
            //并且在调用这个initCurator()函数的时候，除了第一次调用进行初始化之外，一定是对这个临时节点操作时,或者当前连接断开重新连接时，找不到这个节点才会报错，这时候再创建一个节点就可以
            if (myPath ==null || myPath.equals("") || curatorFramework.checkExists().forPath(myPath) == null){
                Long numL = 0L;
                byte[] numLbytes = IntLong2BytesUtil.long2Bytes(numL);
                myPath = curatorFramework.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(rootPath+"/OSN",numLbytes);
            }
            //当前存的所有节点的sum的刷新  所有节点的路径的刷新
            initClientValues();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    /**
     * root路径下的节点的路径和所有节点的存的数的和  的初始化
     */
    public  void initClientValues(){
        try {
            //获取所有子节点的路径  这里的路径是不包含上一级的路径的不全路径 例如 全路径是  rootPath/myPath   这里获取的是 myPath的List<String>
            getKidsPathUnderRootIn();
            //为了防止操作过程中kidsPathUnderRoot被改动
            List<String> kidsPathes = kidsPathUnderRoot;
            Long numCount = 0L;
            for (String pathes:kidsPathes) {
                //加个异常处理，没有就不要了
                numCount = numCount + IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath(rootPath+"/"+pathes));
            }
            zkServerCurrentNumL = numCount;
            //获取当前连接的zk地址
            currentConnectString = curatorFramework.getZookeeperClient().getCurrentConnectionString();
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
        }catch (KeeperException.NoNodeException e) {
            //这里异常是这个节点可能被删除了，重新建立这个节点
            log.error(myPath+" 节点可能被错误删除，正在重新建立节点......"+e.getMessage(),e);
            initCuratorNodes();
            addMyNum2NodeValue(num);
        }catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    /**
     * 给内部调用
     * 获取所有子节点的路径  这里的路径是不包含上一级的路径的不全路径 例如 全路径是  rootPath/myPath   这里获取的是 myPath的List<String>
     * @return
     */
    private  void getKidsPathUnderRootIn() {
        try {
            kidsPathUnderRoot  = curatorFramework.getChildren().forPath(rootPath);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    /**
     * 给外部调用，获取当前的根节点下的路径
     * @return
     */
    public static List<String> getKidsPathUnderRootOut() {
        return kidsPathUnderRoot;
    }

    public static void setKidsPathUnderRoot(List<String> kidsPathUnderRoot) {
        CuratorClient.kidsPathUnderRoot = kidsPathUnderRoot;
    }

    /**
     * 获取root路径下的所有节点的数的和
     * @return
     */
    private   Long getZkServerCurrentNumLIn() {
        initClientValues();
        return zkServerCurrentNumL;
    }

    /**
     * 供外部获取当前所有节点的和
     * @return
     */
    public static  Long getZkServerCurrentNumLOut(){
        return zkServerCurrentNumL;
    }

    public void setZkNodeValue0(){
        Long numL = 0L;
        byte[] numLbytes = IntLong2BytesUtil.long2Bytes(numL);
        try {
            curatorFramework.setData().withVersion(-1).forPath(myPath, numLbytes);
        } catch (KeeperException.NoNodeException e) {
            //这里异常是这个节点可能被删除了，重新建立这个节点
            log.error(myPath+" 节点可能被错误删除，正在重新建立节点......"+e.getMessage(),e);
            initCuratorNodes();
            setZkNodeValue0();
        }catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

//    public static void setZkServerCurrentNumL(Long zkServerCurrentNumL) {
//        CuratorClient.zkServerCurrentNumL = zkServerCurrentNumL;
//    }

    public static String getRootPath() {
        return rootPath;
    }

    public static void setRootPath(String rootPath) {
        CuratorClient.rootPath = rootPath;
    }


    public static String getMyPath() {
        return myPath;
    }

    public static void setMyPath(String myPath) {
        CuratorClient.myPath = myPath;
    }

    public  String getConnectZkUrlPort() {
        return connectZkUrlPort;
    }

    public  void setConnectZkUrlPort(String ZkUrlPort) {
        connectZkUrlPort = ZkUrlPort;
    }

    public static void setCuratorClient(CuratorFramework curatorFramework) {
        CuratorClient.curatorFramework = curatorFramework;
    }

    /**
     * 内部类，用来检测连接状态,并在连接自己的节点连接不上的时候，能去连接其他服务器
     */
        class MyConnectionStateListener implements ConnectionStateListener {

        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            if (connectionState == ConnectionState.LOST||connectionState == ConnectionState.SUSPENDED){
                log.info("连接丢失或挂起，设置连接状态为false，开放访问开关...");
                connectToServer.set(false);
                setOn();
            }else if (connectionState == ConnectionState.RECONNECTED){
                // 重新连接之后，之前的临时节点将被删除，重新建立一个新的节点
                log.info("重新连接成功，设置连接状态为true,并且初始化节点和值...");
                initCuratorNodes();
                connectToServer.set(true);
            }
        }
    }

    /**
     * 内部类，用来处理zookeeper的节点值
     * 如果本地缓存有值，那么把它加进zookeeper节点里，如果没有，那么
     */
    class DealZkNodes implements Runnable{

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
                  /*
        * 如果myNum>0，将它设置为0，然后往zookeeper的节点里加一次，并判断当前节点下的所有的访问量是否超限
        * 如果超限，就关闭访问开关，如果没有就
        * */
            while (true) {
                Integer num = myNum.get();
                myNum.set(0);
                // 如果此时是连接到服务器的，则对本地的缓存数据进行处理，否则不做处理
                if (connectToServer.get()){
                    try {
                        if(isOnOff() == false){
                            num = 0;
                        }
                        if (getZkServerCurrentNumLIn() < maxVisitValue) {
                            setOn();
                            if (num > 0){
                                log.info("连接服务器正常,num = "+num+"，处理本地缓存访问次数，向服务器同步...");
                                addMyNum2NodeValue(num);
                            }
                        } else {
                            //这时候在固定时间内已经超过最大限制数量，休眠些许时间
                            log.info("连接服务器正常，在固定时间内已经超过最大限制数量，休眠些许时间...");
                            setOff();
                            Thread.sleep(overLimitSleepMS);
                        }
                    } catch (InterruptedException e) {
                        log.error("队列操作-----线程休眠出错 ："+e.getMessage(),e);
                    } catch (Exception e) {
                        log.error(e.getMessage(),e);
                    }
                }
            }
        }
    }

    /**
     * 建立对zookeeper的连接，如果建立成功则返回true
     * 否则返回false
     * @return
     */
    public boolean initConnect(String ZkUrlPort){
        connectZkUrlPort = ZkUrlPort;
        boolean initResult = false;
        RetryPolicy retryPolicy = new RetryForever(3000);
//        RetryPolicy retryPolicy = new ExponentialBackoffRetry(5000, 0);
        curatorFramework = CuratorFrameworkFactory.builder().connectString(ZkUrlPort)
                .retryPolicy(retryPolicy).namespace("flBase").connectionTimeoutMs(4000)
                .build();
        curatorFramework.start();
        try {
            initResult = curatorFramework.blockUntilConnected(4500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(),e);
        }
        return initResult;
    }

    /**
     * 开始流量控制的相关步骤
     *
     */
    public void flClrStart(){
        try {
            MyConnectionStateListener stateListener = new MyConnectionStateListener();
            curatorFramework.getConnectionStateListenable().addListener(stateListener);
            //初始化流控节点
            initCuratorNodes();
            //启动处理myNum（本地缓存访问数的线程）
            new Thread(new DealZkNodes()).start();
            //启动一个timer 每隔一段时间去设置zkNodes为0
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                public void run() {
                    if (connectToServer.get()){
                        log.info("连接服务器正常，超过"+flTimeSpanMS+"毫秒，设置节点为0......");
                        curatorClient.setZkNodeValue0();
                    }
                }
            }, 5000,flTimeSpanMS);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }
}
