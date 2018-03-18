package com.nlpt.flowcontrol.entity;

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

        curatorClient.setLeader();

        System.out.println("我是leader！");

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

        curatorClient.setNotLeader();
        System.out.println("我失去leader了!");
    }

    public String getLeaderPath() {
        return leaderPath;
    }

}
