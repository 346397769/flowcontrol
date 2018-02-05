package com.example.flowcontrol.entity;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

        while (curatorClient.getConnectToServer()){
            //检查需要被删除的维度的线程有没有停止，没停止的话就删除
            for (String s : curatorClient.getNeedToBeDeleteDimensions()){
                if (curatorClient.getRunningThraedMap().get(s).getState() != Thread.State.TERMINATED){
                    //提醒该线程需要结束了
                    curatorClient.getRunningThraedMap().get(s).interrupt();
                }else {
                    //从正在运行的线程map中把它删除
                    curatorClient.getRunningThraedMap().remove(s);
                    //从需要被删除的维度中移除
                    curatorClient.getNeedToBeDeleteDimensions().remove(s);
                }
            }

            Thread.sleep(60000);
        }

    }

    public String getLeaderPath() {
        return leaderPath;
    }

}
