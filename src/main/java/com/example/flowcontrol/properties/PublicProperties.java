package com.example.flowcontrol.properties;

public class PublicProperties {
    //要连接的服务端ip 端口
    public static final String CONNECT_ZK_URL_PORT = "10.124.134.37:2181,10.124.134.38:2181,10.124.134.39:2181,10.124.128.195:2181,10.124.128.196:2181";
//    //客户端放在对应的服务器机器上时用这个
//    public static final String CONNECT_ZK_URL_PORT = "127.0.0.1:2181";
    //流控测试根节点
    public static final String FL_TEST_NODE_PATH = "/flCtrlTest";
}
