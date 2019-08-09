package org.nmq.zookeeper;

import org.I0Itec.zkclient.ZkClient;

/**
 * @author niemengquan
 * @create 2019/8/8
 * @modifyUser
 * @modifyDate
 */
public class ZKClientTest {
    /**
     * zk连接地址
     */
    private static final String CONNECTSTRING = "127.0.0.1:2181";

    public static void main(String[] args) {
        // 创建zk连接
        ZkClient zkClient = new ZkClient(CONNECTSTRING);
        zkClient.createEphemeral("/LOCK/APP_");
        zkClient.close();
    }
}
