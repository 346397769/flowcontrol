package com.example.flowcontrol.properties;

public class PublicProperties {
    //固定时间内可以请求的次数
    public static final Long MAX_VALUE = 500L;
    //固定时间长度 毫秒
    public static final Long MAX_INTERVAL_MS = 1000L;
    //队列没有待添加的数的时候，休眠的时间 毫秒
    public static final Long QUEUE_NO_VALUE_SLEEP_MS = 100L;
    //要连接的服务端ip 端口
    public static final String CONNECT_ZK_URL_PORT = "10.124.134.37:2181";
//    //客户端放在对应的服务器机器上时用这个
//    public static final String CONNECT_ZK_URL_PORT = "127.0.0.1:2181";
    //流控测试根节点
    public static final String FL_TEST_NODE_PATH = "/flCtrlTest";
}
