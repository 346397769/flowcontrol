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

    //保存当前运行的所有的<维度，线程>的map
    private Map<String,Thread> settingZeroThraedMap = new ConcurrentHashMap<String,Thread>();

    public LeaderSelectorClient(CuratorClient curatorClientIn,String path){

        leaderPath = path;

        curatorClient = curatorClientIn;

        List<String> initStrings = new ArrayList<>();

        for (String s : curatorClient.getDimensionFlctrlCurrentHashMap().keySet()){
            initStrings.add(s);
        }

        initSetingZeroThread(initStrings,new ArrayList<String>());

        leaderSelector = new LeaderSelector(curatorClient.getCuratorFramework(), path, this);
        //保证此实例在释放领导权后还可能获得领导权
        leaderSelector.autoRequeue();
    }

    public void initSetingZeroThread(List<String> add,List<String> decrease){
        for (String s : add){
            settingZeroThraedMap.put(s,new Thread(new SetingZero(curatorClient.getDimensionFlctrlCurrentHashMap().get(s))));
        }

        for (String s : decrease){
            settingZeroThraedMap.remove(s);
        }
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
        if (curatorClient.getConnectToServer()){
            //检查需要被删除的维度的线程有没有停止，没停止的话就删除
            for (String s : curatorClient.getNeedToBeDeleteDimensions()){
                if (curatorClient.getRunningThraedMap().get(s).getState() != Thread.State.TERMINATED){
                    //提醒该线程需要结束了
                    curatorClient.getRunningThraedMap().get(s).interrupt();
                }else {
                    //从正在运行的线程map中把它删除
                    curatorClient.getRunningThraedMap().remove(s);
                    //从需要被删除的维度中移除
                    curatorClient.getRunningThraedMap().remove(s);
                }
            }

            long maxSleepTime = 0;
            //在每个维度的下，设置维度节点下所有的临时叶子节点为0
            for (FlControlBean flControlBean : curatorClient.getDimensionFlctrlCurrentHashMap().values()){
                if (maxSleepTime < flControlBean.getFlTimeSpanMS())
                {
                    maxSleepTime = flControlBean.getFlTimeSpanMS();
                }

                settingZeroThraedMap.get(flControlBean.getDimension()).start();

//                long timeLongMS = new Date().getTime();
//                if (timeLongMS - flControlBean.getLastTimeSet02MyTempZkNode() >= flControlBean.getFlTimeSpanMS()){
//                    curatorClient.setDimension0(flControlBean.getDimension());
////                    curatorClient.deleteDimensionNodes(flControlBean);
//                    flControlBean.setLastTimeSet02MyTempZkNode(timeLongMS);
//                }
            }

            Thread.sleep(maxSleepTime);
        }

    }

    public String getLeaderPath() {
        return leaderPath;
    }


    class SetingZero implements Runnable{

        private FlControlBean flControlBean;

        public SetingZero(FlControlBean flControl){
            flControlBean = flControl;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(flControlBean.getFlTimeSpanMS());
                curatorClient.setDimension0(flControlBean.getDimension());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
