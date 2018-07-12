package com.nlpt.flowcontrol.entity;

import org.apache.zookeeper.client.FourLetterWordMain;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

/**
 * zookeeper Server类
 * 提供zookeeper服务端的服务，如果使用这个类，那么就不需要再重新部署一套zookeeper Server 服务端
 * 通过修改 resources/conf/zoo.cfg 文件来修改配置
 * @Author 史永飞
 */
public class ZkServer implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(QuorumPeerMain.class);
    private static Properties zoo_properties = new Properties();
    private Thread ts = null;

    public ZkServer(Properties properties){
        zoo_properties = properties;
    }

    public  void  startClusterZkServer(){
        if (ts == null || ts.getState() == Thread.State.TERMINATED){
            ts = new Thread(this);
            ts.start();
        }
    }

//    public void stopZkServer(){
//        if (ts != null ){
//            //   && ts.getState() != Thread.State.TERMINATED  && getZkServerStartStatus()
//            //   没有加上面的两条判定，因为就算没有在运行，也可以执行停止命令
////        ts.stop();
//            ts.interrupt();
//        }
//    }

    /**
     * 使用 FourLetterWordMain.send4LetterWord 获取当前状态
     * 如果是正常运行的 应该是如下字符串
     * Zookeeper version: 3.4.6-1569965, built on 02/20/2014 09:09 GMT
     * Latency min/avg/max: 0/0/0
     * Received: 1
     * Sent: 0
     * Connections: 1
     * Outstanding: 0
     * Zxid: 0x2
     * Mode: standalone
     * Node count: 4
     * @return  leader  follower  standalone
     */
    public String getZkServerMode(){
        String modeString = null;
        try {
            modeString = FourLetterWordMain.send4LetterWord("127.0.0.1",Integer.parseInt(zoo_properties.getProperty("clientPort")),"srvr");
            int a = modeString.indexOf("Mode");
            int b = modeString.indexOf("Node");
            if (a != -1 && b != -1){
                modeString = modeString.substring(a,b-1);
            }
        } catch (IOException e) {
            log.error(e.getMessage(),e);
        }
        return modeString;
    }

    /**
     * 获取当前的zk服务端线程状态
     * @return
     */
    public Thread.State serverState(){
        return ts.getState();
    }

    /**
     * 查看当前是否已经运行了zk服务端
     * @return
     */
    public boolean getZkServerStartStatus() throws IOException {
        boolean status = true;
        Socket sock = null;
        try{
            sock = new Socket("127.0.0.1", Integer.parseInt(zoo_properties.getProperty("clientPort")));

        }catch (java.net.ConnectException e){
            status = false;
            log.error("嵌入式zk服务端没有启动或者启动失败："+e.getMessage(),e);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }finally {
            sock.close();
        }
        return status;
    }

    @Override
    public void run() {
        try {
            // 读取配置文件加载配置，如果直接在构造函数中穿入的含参数的 Properties 对象，以下三行就可以注释掉
            // 或者可以将这三行摘出去，用来初始化 Properties 对象
//            InputStream zooInput = ZkServer.class.getResourceAsStream("/conf/zoo.cfg");
//            zoo_properties.load(zooInput);
//            zooInput.close();
            //以上三行

            QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
            quorumConfig.parseProperties(zoo_properties);

            DatadirCleanupManager purgeMgr = new DatadirCleanupManager(quorumConfig.getDataDir(), quorumConfig.getDataLogDir(), quorumConfig.getSnapRetainCount(), quorumConfig.getPurgeInterval());
            purgeMgr.start();

            // 如果配置了多个server，那么就启动集群模式
            // 否则启动单机模式
            if (quorumConfig.getServers().size() > 0) {
                QuorumPeerMain peer = new QuorumPeerMain();
                peer.runFromConfig(quorumConfig); // To start the replicated server
                log.info("zookeeper服务端集群模式启动");
            } else {
                log.warn("zookeeper服务端单机模式启动");
                ZooKeeperServerMain zooKeeperServerMain = new ZooKeeperServerMain();
                ServerConfig serverConfig = new ServerConfig();
                serverConfig.readFrom(quorumConfig);
                zooKeeperServerMain.runFromConfig(serverConfig);
            }
        } catch (Exception e){
            log.error(e.getMessage(),e);
        }
    }
}

