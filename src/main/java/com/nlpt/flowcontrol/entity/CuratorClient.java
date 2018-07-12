package com.nlpt.flowcontrol.entity;
import com.nlpt.flowcontrol.utils.IntLong2BytesUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.*;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *  流控需要实例化这个类，并调用其中的两个方法
 * 1. initFl  初始化流控节点
 * 2. doFlowControl 根据初始化的节点进行流控
 *
 */
public class CuratorClient{
    private static final Log log = LogFactory.getLog(CuratorClient.class);

    //要创建的根节点路径  也就是Client使用的namespace
    private String rootPath;

    //创建的属于自己的目录
    private String myPath;

    //流控客户端是否连接到服务器
    private AtomicBoolean connectToServer = new AtomicBoolean(false);

    //要连接的zk的url和端口
    private  String connectZkUrlPort;

    //curator的客户端
    private CuratorFramework curatorFramework = null;

    //当前的维度和每个维度对应的FlControlBean
    private Map<String,FlControlBean> dimensionFlctrlCurrentHashMap = new ConcurrentHashMap<String,FlControlBean>();

    //保存当前运行的所有的<维度，线程>的map
    private Map<String,Thread> runningThraedMap = new ConcurrentHashMap<String,Thread>();

    //timerTaskMap 保存作为leader执行的定时任务的map
    private Map<String,SettingZero> timerTaskMap = new ConcurrentHashMap<String,SettingZero>();

    //初始化完节点的标志，以供成为leader之后的定时任务执行
    private AtomicBoolean initNodesDoneFlag = new AtomicBoolean(false);

    //获取leader时的锁，该锁将会在失去leader时被释放
    private String LeaderLock = "LEADER_LOCK";

    //leader选举客户端
    private LeaderSelectorClient leaderSelectorClient = null;

    /**
     * 删除 flControlBean的维度下面的所有的临时节点，相当于重置为0
     */
    public void deleteDimensionNodes(FlControlBean flControlBean){
        try {
            // 如果此时有异常情况导致维度节点不存在，那么就返回，不执行下面的操作
            if (curatorFramework.checkExists().forPath("/"+flControlBean.getDimension()) == null){
                return;
            }
            List<String> pathes = getKidsPathUnderRootIn("/"+flControlBean.getDimension());
            for (String path : pathes){
                curatorFramework.delete().forPath("/"+flControlBean.getDimension()+"/"+path);
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    public String getLeaderLock() {
        return LeaderLock;
    }

    public boolean isInitNodesDoneFlag() {
        return initNodesDoneFlag.get();
    }


    public Map<String, FlControlBean> getDimensionFlctrlCurrentHashMap() {
        return dimensionFlctrlCurrentHashMap;
    }

    public Map<String, Thread> getRunningThraedMap() {
        return runningThraedMap;
    }


    public CuratorFramework getCuratorFramework() {
        return curatorFramework;
    }

    public boolean getConnectToServer(){
        return connectToServer.get();
    }

    //根据命名空间和连接串生成实例
    public CuratorClient(String nameSpace,String connectZkUrlPort,String myPath){
        this.rootPath = nameSpace;
        this.connectZkUrlPort = connectZkUrlPort;
        this.myPath = myPath;
    }

    /**
     * 增加本地结点
     * 包括1.增加到本地维度map中  2.创建本地到远程的同步线程,并把它增加到runningThraedMap中进行管理  3.增加定时任务（leader）
     * @param flControlBean
     */
    private void addLocalDimensionNodes(FlControlBean flControlBean){
        FlControlBean flControlBeanInit = new FlControlBean(flControlBean.getDimension(),flControlBean.getMaxVisitValue(),flControlBean.getFlTimeSpanMS());
        flControlBeanInit.setMyPath("/"+flControlBeanInit.getDimension()+"/"+myPath);
        dimensionFlctrlCurrentHashMap.put(flControlBeanInit.getDimension(),flControlBeanInit);
        //不用添加叶子节点，在请求的时候会自动添加节点
        Thread ts = new Thread(new DealZkNodes(flControlBean.getDimension()));
        runningThraedMap.put(flControlBean.getDimension(),ts);
        ts.start();
        addTimerTask(flControlBean.getDimension());
    }

    /**
     * 删除本地结点
     * 包括 1.删除本地map 2.停止并删除本地同步线程 3.删除定时任务(leader)
     * @param dimension
     */
    private void deleteLocalDimensionNodes(String dimension){
        //将需要删除的维度map删除，需要停止的维度线程停止
        dimensionFlctrlCurrentHashMap.remove(dimension);

        //检查需要被删除的维度的线程有没有停止，没停止的话就删除
        if (getRunningThraedMap().get(dimension)!=null){
            if (getRunningThraedMap().get(dimension).getState() != Thread.State.TERMINATED){
                //提醒该线程需要结束了
                getRunningThraedMap().get(dimension).interrupt();
            }
            //从正在运行的线程map中把它删除
            getRunningThraedMap().remove(dimension);
        }
        cancelTimerTask(dimension);
    }

    /**
     * 增加远程结点
     * @param dimension
     */
    private boolean addRemoteDimensionNodes(String dimension){
        boolean createResult = false;
        //创建新增的根节点
        try {
            if (leaderSelectorClient.isLeader() && curatorFramework.checkExists().forPath("/"+dimension) == null && dimensionFlctrlCurrentHashMap.get(dimension) != null){
                curatorFramework.create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath("/"+dimension,dimension.getBytes());
                createResult = true;
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
        return createResult;
    }

    /**
     * 删除远程结点
     * @param dimension
     */
    private void deleteRemoteDimensionNodes(String dimension) throws Exception {
        if (leaderSelectorClient.isLeader() && curatorFramework.checkExists().forPath("/"+dimension) != null && dimensionFlctrlCurrentHashMap.get(dimension) == null){
            curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath("/"+dimension);
        }
    }


    /**
     * 初始化zookeeper的节点
     * 并设置初始值
     *
     * 建立维度根节点的策略是：
     *      以传入维度为基准，更新本地结点和远程结点
     *       已有的维度集合 = listA
     *      传入的维度集合 = listB
     *      需要删除的维度节点 = listA - listB
     *      需要新建的节点 = listB - listA
     */
    private void initCuratorNodes(List<FlControlBean> flControlBeans){
        try {
            // 更新本地和zk已有结点属性 和 补偿创建线程任务和定时任务
            updateNodes(flControlBeans);
            // 获取传入list，zookeeper结点list，本地list

            // 本地list
            List<String> listLocal = new ArrayList<String>();

            // zk结点list
            List<String> listZk = getKidsPathUnderRootIn("/");
            listZk.remove("leader");

            // 传入list
            List<String> listIntro = new ArrayList<String>();

            // 获取本地list
            for (String s : dimensionFlctrlCurrentHashMap.keySet()){
                listLocal.add(s);
            }

            //获取传入list
            for (FlControlBean flControlBean:flControlBeans){
                listIntro.add(flControlBean.getDimension());
            }

            // 分别对三个list做copy 以便下面的计算
            List<String> copyOfListLocal = new ArrayList<String>(listLocal);
            List<String> copyOfListZk = new ArrayList<String>(listZk);
            List<String> copyOfListIntro = new ArrayList<String>(listIntro);

            // 传入list和本地list比较，对本地list进行增删
            // 本地需要删除的结点
            listLocal.removeAll(listIntro);
            // 本地需要增加的结点
            listIntro.removeAll(copyOfListLocal);

            // 传入list和zookeeper结点比较，对zookeeper结点进行增删

            // zk需要删除的结点
            listZk.removeAll(copyOfListIntro);
            // zk需要增加的结点
            copyOfListIntro.removeAll(copyOfListZk);

            // 更新zk结点
            // 新增
            for (String s:copyOfListIntro) {
                addRemoteDimensionNodes(s);
            }
            // 删除
            for (String s:listZk) {
                deleteRemoteDimensionNodes(s);
            }

            // 更新本地结点
            // 新增
            for (String s : listIntro) {
                for (FlControlBean flControlBean:flControlBeans) {
                    if (flControlBean.getDimension().equals(s)){
                        addLocalDimensionNodes(flControlBean);
                    }
                }
            }
            // 删除
            for (String s:listLocal) {
                deleteLocalDimensionNodes(s);
            }


        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
    }

    /**
     * 获取某一个维度的阈值
     * @param dimension
     * @return
     */
    public int getFcThreshold(String dimension){
        if (dimensionFlctrlCurrentHashMap.get(dimension) != null){
            return dimensionFlctrlCurrentHashMap.get(dimension).getMaxVisitValue();
        }else {
            return -1;
        }
    }

    /**
     * 将传入的List跟已有的维度进行比较
     * 如果有不同，那么根据不同去进行更改
     * 只有可能是MaxVisitValue改变，因为，传入的维度都加了_M,_S,_D,_H等后缀，所以每一种类型的FlTimeSpanMS都是固定的
     * 并将 runningThraedMap  dimensionFlctrlCurrentHashMap 拉齐
     * 补偿创建master结点的定时任务
     * @param flControlBeans
     */
    private void updateNodes(List<FlControlBean> flControlBeans){
        for (FlControlBean flControlBean : flControlBeans){
            if (dimensionFlctrlCurrentHashMap.get(flControlBean.getDimension()) != null){
                if (dimensionFlctrlCurrentHashMap.get(flControlBean.getDimension()).getMaxVisitValue() != flControlBean.getMaxVisitValue()){
                    dimensionFlctrlCurrentHashMap.get(flControlBean.getDimension()).setMaxVisitValue(flControlBean.getMaxVisitValue());
                }
                //只有可能是MaxVisitValue改变，因为，传入的维度都加了_M,_S,_D,_H等后缀，所以每一种类型的FlTimeSpanMS都是固定的
                //但是为了普遍的使用其他的使用者，所以这里FlTimeSpanMS也是支持修改的
                if (dimensionFlctrlCurrentHashMap.get(flControlBean.getDimension()).getFlTimeSpanMS() != flControlBean.getFlTimeSpanMS()){
                    dimensionFlctrlCurrentHashMap.get(flControlBean.getDimension()).setFlTimeSpanMS(flControlBean.getFlTimeSpanMS());
                    cancelTimerTask(flControlBean.getDimension());
                    addTimerTask(flControlBean.getDimension());
                }
                //将 runningThraedMap  dimensionFlctrlCurrentHashMap 拉齐
                if (runningThraedMap.get(flControlBean.getDimension()) == null && dimensionFlctrlCurrentHashMap.get(flControlBean.getDimension()) != null){
                    Thread ts = new Thread(new DealZkNodes(flControlBean.getDimension()));
                    runningThraedMap.put(flControlBean.getDimension(),ts);
                    ts.start();
                }
                // 补偿创建master结点的任务，如果是master并且已经存在此任务，那么就不会创建
                addTimerTask(flControlBean.getDimension());
            }
        }
    }

    /**
     * 获取某个维度路径下的所有节点的存的数的和
     */
    public long getZkServerCurrentNumLIn(FlControlBean flControlBean){

        // 如果此时有异常情况导致维度节点不存在，那么就返回，不执行下面的操作
        try {
            if (curatorFramework.checkExists().forPath("/"+flControlBean.getDimension()) == null){
                return 0;
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }

        //获取所有子节点的路径  这里的路径是不包含上一级的路径的不全路径 例如 全路径是  /dimension/myPath   这里获取的是 myPath的List<String>
        List<String> kidsPathes = getKidsPathUnderRootIn("/"+flControlBean.getDimension());
        long numCount = 0L;
        for (String pathes:kidsPathes) {
            //加个异常处理，没有这个节点就不要统计了，去统计下一个
            //getNodeValue 用这个方法
            try {
                numCount = numCount + getNodeValue("/"+flControlBean.getDimension()+"/"+pathes);
            }catch (Exception e) {
                // 这里报异常的可能是，节点断线，节点被定时任务删除，维度节点由于leader异常消失时进行增加而不存在
                // 但是这里不需要重建节点，因为在 addMyNum2NodeValue 的时候已经检查重建了
                // 报异常是正常情况
//                log.error("获取节点"+pathes+"数据失败",e);
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
    public long getNodeValue(String path) throws Exception {
        return IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath(path));
    }


    /**
     * 往zkNode节点+当前缓存的本地访问数
     * org.apache.zookeeper.KeeperException$BadVersionException 这个异常表示节点版本号不对
     */
    private void addMyNum2NodeValue(FlControlBean flControlBean,int num){
        try {
//            int version = curatorFramework.checkExists().forPath(flControlBean.getMyPath()).getVersion();
//            long currentNum = IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath(flControlBean.getMyPath()));
//            curatorFramework.setData().withVersion(version).forPath(flControlBean.getMyPath(),IntLong2BytesUtil.long2Bytes(currentNum+num));
            // 如果某一时刻由于leader不存在，导致只初始化了 dimensionFlctrlCurrentHashMap 而没有创建节点，那么在这里拉齐创建

            if (curatorFramework.checkExists().forPath("/"+flControlBean.getDimension()) == null){
                return;
            }

            long currentNum = IntLong2BytesUtil.bytes2Long(curatorFramework.getData().forPath(flControlBean.getMyPath()));
            curatorFramework.setData().forPath(flControlBean.getMyPath(),IntLong2BytesUtil.long2Bytes(currentNum+num));
        }catch (KeeperException.NoNodeException e) {
            if (checkAndRecreateNodes(flControlBean.getDimension())){
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
    private  List<String> getKidsPathUnderRootIn(String path) {
        List<String> kidsPathUnderDimension = new ArrayList<>();
        try {
            kidsPathUnderDimension  = curatorFramework.getChildren().forPath(path);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
        return kidsPathUnderDimension;
    }

    /**
     * 此方法检查节点的父节点是否存在，如果存在，那么重建子节点返回true，如果不存在，那么不操作返回false。
     * @param path 维度
     * @return
     */
    private boolean checkAndRecreateNodes(String path){
        boolean exist = false;

        try {
            if(dimensionFlctrlCurrentHashMap.get(path) != null){

                // 由于leader的定时任务删除的子节点，在这里创建
                if (curatorFramework.checkExists().forPath("/"+path) != null){
                    exist = true;
                    long numL = 0L;
                    byte[] numLbytes = IntLong2BytesUtil.long2Bytes(numL);
                    curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(dimensionFlctrlCurrentHashMap.get(path).getMyPath(),numLbytes);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
        return exist;
    }

    public String getMyPath() {
        return myPath;
    }

    public void setMyPath(String myPath) {
        this.myPath = myPath;
    }

    public String getConnectZkUrlPort() {
        return connectZkUrlPort;
    }


    /**
     * 内部类，用来检测连接状态,并在连接自己的节点连接不上的时候，能去连接其他服务器
     */
    class MyConnectionStateListener implements ConnectionStateListener {

        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            if (connectionState == ConnectionState.LOST||connectionState == ConnectionState.SUSPENDED){
                closeConnect2Zk();
                log.info("连接丢失或挂起，设置连接状态为false，释放leader");
            }else if (connectionState == ConnectionState.RECONNECTED || connectionState == ConnectionState.CONNECTED){
                // 重新连接之后，之前的临时节点将被删除，重新建立一个新的节点
                // 虽然这里是断开之后重新连接，但是不用对节点进行重新的初始化，因为在set0和add的时候，如果没有相应的临时节点，已经写了基于根节点的自动重建策略
                connectToServer.set(true);
                log.info("连接或重新连接成功，设置连接状态为true");
            }
        }
    }

    /**
     * 内部类，用来处理zookeeper的节点值
     * 如果本地缓存有值，那么把它加进zookeeper节点里，如果没有，那么waiting，等待被唤醒
     */
    class DealZkNodes implements Runnable{

        private boolean flag = true;
        private String dimensionName;

        public DealZkNodes(String s){
            dimensionName = s;
        }

        @Override
        public void run() {
                  /*
                  *  如果还有这个维度，并且没有被要求结束，就一直执行
                    * */
            while (dimensionFlctrlCurrentHashMap.get(dimensionName)!=null && flag) {

                // 如果此时是连接到服务器的，则对本地的缓存数据进行处理，否则不做处理
                if (connectToServer.get()){
                    FlControlBean flControlBean = dimensionFlctrlCurrentHashMap.get(dimensionName);
                    try {
                                /*
                                *  如果本地存储的数是>0的，从本地存储减去当前的数
                                *    去减而不是设置0的好处是，是因为如果有延迟的话,就会出现这种情况：
                                *    从A的到数字，将A设置为0之前，就有相同维度的访问，使得myNum这个数字增加了，这时候我还是会把它设置为0，因此造成误差
                                *    如果是减，就不会有这种误差了
                                *
                                *    但是，减去的话，跟加的时候，操作的是同一个数，高并发下这种操作可能会性能低
                                *    本着宽容策略和性能的角度，设置0
                                * */
                        int num = flControlBean.getMyNum();
                        if(num > 0){
                            //将取到的值从本地存储中的值减去
//                                    flControlBean.decreaseMyNum(num);
                            //这里用set 0  而不是 减去0  有两点原因
                            // 第一 在很大的访问量的时候,set 0 的时候,myNum也正在增加，就会有增加的这部分的上限被set 为0，使得它的设置上限值增加
                            // 第二 myNum这个是线程安全的，但是在得不到应得的正确数值的时候就会造成无限循环，导致效率降低
                            flControlBean.setMyNum(0);
                            //这里之所以用num而不用flControlBean存的数，是因为很可能在使用flControlBean存的数的过程中，它的值又遭到改变
                            addMyNum2NodeValue(flControlBean,num);
                            //如果此时超过限制，那么关闭开关
                            if (flControlBean.getMaxVisitValue() > 0 && getZkServerCurrentNumLIn(flControlBean) > flControlBean.getMaxVisitValue()) {
                                flControlBean.setOff();
                            }
                        }else {
                            synchronized (flControlBean){
                                flControlBean.wait();
                            }
                        }
                    } catch (InterruptedException e){
                        //触发这个异常说明此时已经从dimensionFlctrlCurrentHashMap中把此维度移除，但是这个线程却还没有停止
                        flag = false;
                    }catch (Exception e) {
                        log.error(e.getMessage(),e);
                    }
                }else {
                    //此时连接不上zookeeper那么休息一会
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(),e);
                        flag = false;
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
    public boolean initConnect(){
        boolean initResult = true;
        try {

            RetryPolicy retryPolicy = new RetryForever(3000);
            curatorFramework = CuratorFrameworkFactory.builder().connectString(connectZkUrlPort)
                    .retryPolicy(retryPolicy).namespace(rootPath).connectionTimeoutMs(3000)
                    .build();
            //状态监听
            MyConnectionStateListener stateListener = new MyConnectionStateListener();
            curatorFramework.getConnectionStateListenable().addListener(stateListener);

            leaderSelectorClient = new LeaderSelectorClient(this,"/leader");

            curatorFramework.start();

            leaderSelectorClient.start();

//            initResult = curatorFramework.blockUntilConnected(4500, TimeUnit.MILLISECONDS);
            curatorFramework.blockUntilConnected();
        } catch (InterruptedException e) {
            log.error(e.getMessage(),e);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
        return initResult;
    }

    /**
     * 外围调用的方法，调用此方法会初始化连接,以及一开始的节点初始化
     * @param flControlBeans
     */
    public void initFl(List<FlControlBean> flControlBeans){

        //根据传入的list，初始化流控节点
        initCuratorNodes(flControlBeans);
        //初始化节点成功的flag置为true
        initNodesDoneFlag.set(true);
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
        FlControlBean flControlBean = dimensionFlctrlCurrentHashMap.get(dimension);
        if (flControlBean == null){
            return FlStatus.WRONG_DIMENSION;
        }

        if (flControlBean.getOnOff()){
            wakeThreadAndAddOne(flControlBean);
            return FlStatus.OK;
        }else {
            if (flControlBean.getMaxVisitValue() > 0 && getZkServerCurrentNumLIn(flControlBean) < flControlBean.getMaxVisitValue()) {
                flControlBean.setOn();
                wakeThreadAndAddOne(flControlBean);
                return FlStatus.OK;
            }
            return FlStatus.NO;
        }
    }

    /**
     * 向本地缓存加1，并唤醒自加线程
     * @param flControlBean
     */
    private void wakeThreadAndAddOne(FlControlBean flControlBean){
        flControlBean.addOne2MyNum();
        try {
            if (runningThraedMap.get(flControlBean.getDimension()).getState() == Thread.State.WAITING) {
                synchronized (flControlBean) {
                    flControlBean.notify();
                }
            }
        }catch (NullPointerException e){
            log.error("节点初始化异常，等待拉齐即可",e);
        }
    }

    /**
     * 根据dimension增加timerTask定时任务，用来定时重新初始化（删除）节点
     * @param dimension
     */
    public void addTimerTask(String dimension){
        //当我是leader，并且没有这个维度的定时任务的时候，我才会去添加这个定时任务
        //因为在获得leader的时候，会根据维度去进行定时任务的初始化，同步内存库的时候，也会进行定时任务的初始化，这样避免冲突，在初始化的时候添加了定时任务，
        // 在leader那里就不用重复添加了
        if (leaderSelectorClient.isLeader() && timerTaskMap.get(dimension) == null){
            timerTaskMap.put(dimension,new SettingZero(dimensionFlctrlCurrentHashMap.get(dimension),CuratorClient.this));
            timerTaskMap.get(dimension).startSetting0TimerTask();
        }
    }

    /**
     * 根据dimension取消定时任务，在这个维度被删除或者失去leader的时候执行
     * @param dimension
     */
    public void cancelTimerTask(String dimension){
        //取消维度的定时任务的时候只需要判断timerTaskMap的这个维度是不是null就可以了，因为此时可能已经不是leader，但是任务还需要删除
        if (timerTaskMap.get(dimension)!=null){
            timerTaskMap.get(dimension).cancelTimerTask();
            timerTaskMap.remove(dimension);
        }
    }

    /**
     * 判断当前节点是否是leader节点
     * @return
     */
    public FlStatus isLeader(){
        if (getConnectToServer()){
            if (leaderSelectorClient.isLeader()){
                return FlStatus.IS_LEADER;
            }else {
                return FlStatus.NOT_LEADER;
            }
        }else {
            return FlStatus.LOST_CONNECT;
        }
    }

    /**
     * 主动放弃leader
     */
    public FlStatus interruptLeadership(){
        if (getConnectToServer()){
            if (leaderSelectorClient.isLeader()){
                leaderSelectorClient.interruptLeadership();
                return FlStatus.OK;
            }else {
                return FlStatus.NOT_LEADER;
            }
        }else {
            return FlStatus.LOST_CONNECT;
        }
    }

    /**
     * 统计的当前流量
     * @param dimension
     * @return
     */
    public long getCurrentFlow(String dimension){
        if (dimensionFlctrlCurrentHashMap.get(dimension) != null){
            return getZkServerCurrentNumLIn(dimensionFlctrlCurrentHashMap.get(dimension));
        }else {
            return -1;
        }
    }

    /**
     * 关闭 对 zookeeper的连接
     * 删除定时任务
     */
    public void  stopCurator(){
        try{
            closeConnect2Zk();
            List<String> currentThreadDimensions = new ArrayList<String>();
            for (String s : runningThraedMap.keySet()){
                currentThreadDimensions.add(s);
            }
            for (String s : currentThreadDimensions){
                deleteLocalDimensionNodes(s);
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }finally {
            CloseableUtils.closeQuietly(leaderSelectorClient);
            CloseableUtils.closeQuietly(curatorFramework);
            log.info("关闭对zookeeper的连接，并删除定时任务，结束正在运行的线程，设置连接状态为false");
        }
    }

    /**
     * 断开连接时执行的操作
     */
    private void closeConnect2Zk(){
        try{
            connectToServer.set(false);
            //删除定时任务
            for (FlControlBean flControlBean :dimensionFlctrlCurrentHashMap.values()){
                cancelTimerTask(flControlBean.getDimension());
            }
            synchronized (getLeaderLock()){
                getLeaderLock().notifyAll();
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }
    }
}
