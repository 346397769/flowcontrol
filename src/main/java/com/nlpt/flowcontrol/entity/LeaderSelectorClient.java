package com.nlpt.flowcontrol.entity;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import java.io.Closeable;
import java.io.IOException;

/**
 * CuratorClient 会调用并实例化这个类
 * 此类用于zookeeper客户端的选举，并执行唯一的任务
 */
public class LeaderSelectorClient extends LeaderSelectorListenerAdapter implements Closeable {

    private  LeaderSelector leaderSelector;

    private CuratorClient curatorClient;

    private String leaderPath;

    private static final Log log = LogFactory.getLog(LeaderSelectorClient.class);

    public LeaderSelectorClient(CuratorClient curatorClientIn,String path){

        leaderPath = path;

        curatorClient = curatorClientIn;

        leaderSelector = new LeaderSelector(curatorClient.getCuratorFramework(), path, this);
        //保证此实例在释放领导权后还可能获得领导权
        leaderSelector.autoRequeue();
    }


    public void start()
    {
        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
        leaderSelector.start();
    }

    /**
     * 判断当前节点是不是leader
     * @return
     */
    public boolean isLeader(){
        return leaderSelector.hasLeadership();
    }

    /**
     * 主动放弃leader
     */
    public void interruptLeadership(){
        leaderSelector.interruptLeadership();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            log.error(e.getMessage(),e);
        }
    }



    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        log.info("我是leader！");
        // 将竞选leader的结点的值设置成自定义的表名自己的值 例如：端口+IP
        curatorFramework.setData().forPath(leaderPath,curatorClient.getMyPath().getBytes());

        //给每个维度增加定时任务
        while(true){
            if (curatorClient.isInitNodesDoneFlag()){
                for (FlControlBean flControlBean : curatorClient.getDimensionFlctrlCurrentHashMap().values()){
                    curatorClient.addTimerTask(flControlBean.getDimension());
                }
                break;
            }
            Thread.sleep(1000);
        }

        synchronized (curatorClient.getLeaderLock()){
            curatorClient.getLeaderLock().wait();
        }

        log.info("我失去了leader！");
    }

    public String getLeaderPath() {
        return leaderPath;
    }

}
