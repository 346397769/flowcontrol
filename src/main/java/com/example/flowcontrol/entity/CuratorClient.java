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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CuratorClient{
    private static final Logger log = LoggerFactory.getLogger(CuratorClient.class);

    //要创建的根节点路径  也就是Client使用的namespace
    private static String rootPath = PublicProperties.FL_NODE_ROOT_PATH;

    //超过限制的时候，休眠的时间 毫秒
    private static  Integer overLimitSleepMS = 10;

    //创建的属于自己的目录
    private static String myPath;

    //流控客户端是否连接到服务器
    private static AtomicBoolean connectToServer = new AtomicBoolean(true);

    //要连接的zk的url和端口
    private static  String connectZkUrlPort = PublicProperties.CONNECT_ZK_URL_PORT;

    //curator的客户端
    private static CuratorFramework curatorFramework;

    private static CuratorClient curatorClient = new CuratorClient();

    //当前的维度和每个维度对应的FlControlBean
    private static Map<String,FlControlBean> dimensionFlctrlCurrentHashMap = new ConcurrentHashMap<String,FlControlBean>();


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

    /**
     * 初始化zookeeper的节点
     * 并设置初始值
     *
     * 建立维度根节点的策略是：
     *       已有的维度集合 = listA
     *      传入的维度集合 = listB
     *      需要删除的维度节点 = listA - listB
     *      需要新建的节点 = listB - listA
     */
    private static void initCuratorNodes(List<FlControlBean> flControlBeans){
        try {
            // 首先必须初始化myPath，因为每个维度下都要用这个路径
            if (myPath == null || myPath.equals("")){
                myPath = UUID.randomUUID().toString().replace("-","");
            }
            //传入的维度集合
            List<String> listB = new ArrayList<String>();
            //初始化维度对应map 和 获取传入的维度的集合
            //初始化Map的value和key的List，是因为这样用于建立节点等比较方便
            for (FlControlBean flControlBean:flControlBeans){
                //初始化每个维度的临时统计子节点
                flControlBean.setMyPath("/"+flControlBean.getDimension()+"/"+myPath);
                dimensionFlctrlCurrentHashMap.put(flControlBean.getDimension(),flControlBean);
                listB.add(flControlBean.getDimension());
            }

            //首先获取当前根节点下的所有子节点(也就是当前所有维度)的路径，为了跟传入的值进行比较，并对当前根节点下的子节点进行更新操作（增加，或者删减）
            //获取已有的List<String>类型的维度
            List<String> listA = getKidsPathUnderRootIn("/");

            //已有的维度集合的copy
           List<String> copyListA = new ArrayList<String>(listA);

            //需要删除的维度节点 = listA - listB
            listA.removeAll(listB);

            //需要新建的节点 = listB - listA
            listB.removeAll(copyListA);

            //删除需要删除的维度节点
            //如果是第一次进行初始化，那么应该不会有删减操作
            if (listA.size() > 0){
                for (String s : listA){
                    if (curatorFramework.checkExists().forPath("/"+s) != null){
                        dimensionFlctrlCurrentHashMap.remove(s);
                        curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath("/"+s);
                    }
                }
            }

            //新增需要增加的维度节点,并为其建立临时统计叶子节点
            //如果是第一次进行操作，那么应该是将所有的维度节点进行初始化
            if (listB.size() > 0){
                for (String s : listB){
                    if (curatorFramework.checkExists().forPath("/"+s) == null){
                        curatorFramework.create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath("/"+s,s.getBytes());
                    }
                    //因为是新增的维度根节点，所以不用判断叶子节点是否存在，肯定不存在叶子节点，直接添加就可以
                    Long numL = 0L;
                    byte[] numLbytes = IntLong2BytesUtil.long2Bytes(numL);
                    curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(dimensionFlctrlCurrentHashMap.get(s).getMyPath(),numLbytes);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    /**
     * 获取某个维度路径下的所有节点的存的数的和
     */
    public static Long getZkServerCurrentNumLIn(FlControlBean flControlBean){
            //获取所有子节点的路径  这里的路径是不包含上一级的路径的不全路径 例如 全路径是  rootPath/myPath   这里获取的是 myPath的List<String>
            List<String> kidsPathes = getKidsPathUnderRootIn("/"+flControlBean.getDimension());
            Long numCount = 0L;
            for (String pathes:kidsPathes) {
                //加个异常处理，没有这个节点就不要统计了，去统计下一个
                //getNodeValue 用这个方法
                try {
                    numCount = numCount + getNodeValue("/"+flControlBean.getDimension()+"/"+pathes);
                } catch (Exception e) {
                    log.error("获取节点"+pathes+"数据失败，该节点可能由于客户端断线而已经不存在",e.getMessage(),e);
                }
            }
            return numCount;
    }

    /**
     * 将节点里面存的Long类型的值取出
     * @param path
     * @return
     * @throws Exception
     */
    public static Long getNodeValue(String path) throws Exception {
        return IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath(path));
    }


    /**
     * 往zkNode节点+当前缓存的本地访问数
     * org.apache.zookeeper.KeeperException$BadVersionException 这个异常表示节点版本号不对
     */
    public static void addMyNum2NodeValue(FlControlBean flControlBean,Integer num){
        try {
//            int version = curatorFramework.checkExists().forPath(zkPath).getVersion();
            Long currentNum = IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath(flControlBean.getMyPath()));
            curatorFramework.setData().forPath(flControlBean.getMyPath(),IntLong2BytesUtil.long2Bytes(currentNum+num));
        }catch (KeeperException.NoNodeException e) {
            if (checkAndRecreateNodes(flControlBean.getMyPath())){
                addMyNum2NodeValue(flControlBean,num);
            }else {
                log.error("节点不同步造成的异常"+e.getMessage(),e);
            }
        }catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    /**
     * 获取所有子节点的路径  这里的路径是不包含上一级的路径的不全路径 例如 全路径是  rootPath/myPath   这里获取的是 myPath的List<String>
     *     获取的是某一个维度下的路径
     * @return
     */
    private static   List<String> getKidsPathUnderRootIn(String path) {
        List<String> kidsPathUnderDimension = new ArrayList<>();
        try {
            kidsPathUnderDimension  = curatorFramework.getChildren().forPath(path);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
        return kidsPathUnderDimension;
    }

    /**
     * 将节点的值设置为0
     * @param set0Path
     */
    private void setZkNodeValue0(String set0Path){
        Long numL = 0L;
        byte[] numLbytes = IntLong2BytesUtil.long2Bytes(numL);
        try {
            curatorFramework.setData().forPath(set0Path, numLbytes);
        } catch (KeeperException.NoNodeException e) {
            //这里异常是这个节点可能被删除了,存在两种情况
            // 一种是根节点也被删除了（同步内存库的时候）调用了initFl方法，由于延迟造成了异常，这个时候不需要重建
            // 另一种是仅仅子节点被删除了，这时候说明真的是不明的原因被删除（如果不去服务器人为删除，那么这种可能几乎没有），可以重新建立这个节点，并为它赋值0
            if (checkAndRecreateNodes(set0Path)){
                setZkNodeValue0(set0Path);
            }else {
                log.error("节点不同步造成的异常"+e.getMessage(),e);
            }
        }catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    /**
     * 此方法检查节点的父节点是否存在，如果存在，那么重建子节点返回true，如果不存在，那么不操作返回false。
     * @param path 包含根路径的path
     * @return
     */
    private static boolean checkAndRecreateNodes(String path){
        boolean exist = false;
        String[] pathes = path.split("/");
        try {
            int countPath = 0;
            for (String p : pathes){
                countPath++;
                if (curatorFramework.checkExists().forPath("/"+p) == null){
                    break;
                }
            }
            if (countPath == pathes.length){
                //这时候说明仅仅是子节点没有了，可以重新建立
                exist = true;
                Long numL = 0L;
                byte[] numLbytes = IntLong2BytesUtil.long2Bytes(numL);
                curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(path,numLbytes);
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
        return exist;
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


    /**
     * 内部类，用来检测连接状态,并在连接自己的节点连接不上的时候，能去连接其他服务器
     */
       static  class MyConnectionStateListener implements ConnectionStateListener {

        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            if (connectionState == ConnectionState.LOST||connectionState == ConnectionState.SUSPENDED){
                log.info("连接丢失或挂起，设置连接状态为false，开放所有的访问开关...");
                connectToServer.set(false);
                //开放所有的访问开关
                for (FlControlBean flControlBean :dimensionFlctrlCurrentHashMap.values()){
                    flControlBean.setOnOff(true);
                }
            }else if (connectionState == ConnectionState.RECONNECTED){
                // 重新连接之后，之前的临时节点将被删除，重新建立一个新的节点
                // 虽然这里是断开之后重新连接，但是不用对节点进行重新的初始化，因为在set0和add的时候，如果没有相应的临时节点，已经写了基于根节点的自动重建策略
                log.info("重新连接成功，设置连接状态为true...");
                connectToServer.set(true);
            }
        }
    }

    /**
     * 内部类，用来处理zookeeper的节点值
     * 如果本地缓存有值，那么把它加进zookeeper节点里，如果没有，那么
     */
    static class DealZkNodes implements Runnable{

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
                for (FlControlBean flControlBean :dimensionFlctrlCurrentHashMap.values()){
                    // 如果此时是连接到服务器的，则对本地的缓存数据进行处理，否则不做处理
                    if (connectToServer.get()){
                        try {
                            if(flControlBean.getOnOff()){
                                /*
                                * 如果这里开关是打开的，那么从本地存储减去当前的数
                                * 为什么不用设置为0，而是去减，是因为如果有延迟的话,就会出现这种情况：
                                *    从A的到数字，将A设置为0之前，就有相同维度的访问，使得myNum这个数字增加了，这时候我还是会把它设置为0，因此造成误差
                                *    如果是减，就不会有这种误差了
                                * */
                                Integer num = flControlBean.getMyNum();
                                //将取到的值从本地存储中的值减去
                                flControlBean.decreaseMyNum(num);
                                //这里之所以用num而不用flControlBean存的数，是因为很可能在使用flControlBean存的数的过程中，它的值又遭到改变
                                addMyNum2NodeValue(flControlBean,num);
                            }
                            if (getZkServerCurrentNumLIn(flControlBean) < flControlBean.getMaxVisitValue()) {
                                flControlBean.setOnOff(true);
                            } else {
                                //这时候在固定时间内已经超过最大限制数量，休眠些许时间
//                                log.info("连接服务器正常，在固定时间内已经超过最大限制数量，休眠些许时间...");
                                flControlBean.setOnOff(false);
//                                Thread.sleep(overLimitSleepMS);
                            }
                        } catch (Exception e) {
                            log.error(e.getMessage(),e);
                        }
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
    private static boolean initConnect(String ZkUrlPort){
        connectZkUrlPort = ZkUrlPort;
        boolean initResult = false;
        RetryPolicy retryPolicy = new RetryForever(3000);
//        RetryPolicy retryPolicy = new ExponentialBackoffRetry(5000, 0);
        curatorFramework = CuratorFrameworkFactory.builder().connectString(ZkUrlPort)
                .retryPolicy(retryPolicy).namespace(rootPath).connectionTimeoutMs(4000)
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
    private static void flClrStart(){
        try {
            MyConnectionStateListener stateListener = new MyConnectionStateListener();
            curatorFramework.getConnectionStateListenable().addListener(stateListener);
            //启动处理myNum（本地缓存访问数的线程）
            new Thread(new DealZkNodes()).start();
            //启动一个timer 每隔一段时间去设置zkNodes为0
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                public void run() {
                    if (connectToServer.get()){
                        //在每个维度的下，设置每个维度节点下自己的临时叶子节点为0
//                        log.info("连接服务器正常，超过"+flTimeSpanMS+"毫秒，设置节点为0......");
                        for (FlControlBean flControlBean :dimensionFlctrlCurrentHashMap.values()){
                            Long timeLongMS = new Date().getTime();
                            if (timeLongMS - flControlBean.getLastTimeSet02MyTempZkNode() > flControlBean.getFlTimeSpanMS()){
                                curatorClient.setZkNodeValue0(flControlBean.getMyPath());
                                flControlBean.setLastTimeSet02MyTempZkNode(timeLongMS);
                            }
                        }
                    }
                }
            }, 5000,1000);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    /**
     * 外围调用的方法，调用此方法会初始化连接,以及一开始的节点初始化
     * @param flControlBeans
     */
    public static void initFl(List<FlControlBean> flControlBeans){
        boolean connectSuccess = false;
        //如果曾经初始化过，那么curatorFramework就不会为null，并且不需要再去连接，因为它有自己的集群自动重连操作
        //也就是说，只有第一次初始化操作的时候会满足条件去初始化连接
        if(curatorFramework == null){
            connectSuccess = initConnect(connectZkUrlPort);
        }
        //根据传入的list，初始化流控节点
        initCuratorNodes(flControlBeans);
        //如果连接成功，那么开始流控
        if (connectSuccess){
            flClrStart();
        }
    }

    /**
     * 根据某一个维度进行流量控制
     * @param dimension 根据什么维度进行流量控制
     * @return FlStatus.OK 表示正常流控允许访问，FlStatus.NO 表示正常流控，拒绝访问，FlStatus.LOST_CONNECT表示与服务器失去连接
     * FlStatus.WRONG_DIMENSION 表示没有这个维度
     */
    public FlStatus doFlowControl(String dimension){
        if (!connectToServer.get()){
            return FlStatus.LOST_CONNECT;
        }
        if (dimensionFlctrlCurrentHashMap.get(dimension) == null){
           return FlStatus.WRONG_DIMENSION;
        }
        if (dimensionFlctrlCurrentHashMap.get(dimension).getOnOff()){
            dimensionFlctrlCurrentHashMap.get(dimension).addOne2MyNum();
            return FlStatus.OK;
        }else {
            return FlStatus.NO;
        }
    }
}
