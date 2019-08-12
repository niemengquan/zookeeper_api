package org.nmq.zookeeper.lock;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;
import java.util.concurrent.CountDownLatch;

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
    private static final int SESSION_TIMEOUT = 1000;

    public static void main(String[] args) throws InterruptedException {
        // 创建zk连接
        ZkClient zkClient = null;
        try {
            // 创建zk连接
            zkClient = new ZkClient(CONNECTSTRING,SESSION_TIMEOUT);

            //创建持久化的节点,内容为空
            zkClient.createPersistent("/LOCK/APP_");

            // 获取节点内容
            Object obj = zkClient.readData("/LOCK/APP_");
            System.out.println(obj == null);

            // 删除节点
            zkClient.delete("/LOCK/APP_");

            //创建临时节点
            zkClient.createEphemeral("/LOCK/APP_");

            // 很多增删改查的api都有封装，可以查阅ZkClient的源码查看。下面我们主要看一下Zkclient Watch的使用
            /**
             * 通过使用我们发现。Zkclient里并没有暴露watcher、watch参数，这就是Zkclient帮助我们封装了反复的去注册watcher的问题。开发
             * 人员无需再去关心这点，Zkclient 给我们提供了一套监听方式，我们可以使用监听节点的方式进行操作，剔除了繁琐的反复watcher操
             * 作、简化了代码的复杂程度，这点做得非常不错。
             *
             * subscribeChildChanges方法 订阅子节点变化
             */
            zkClient.subscribeChildChanges("/parent", new IZkChildListener() {
                @Override
                public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                    System.out.println("parentPath: " + parentPath);
                    System.out.println("currentChilds: " + currentChilds);
                }
            });
            zkClient.createPersistent("/parent");
            zkClient.createEphemeral("/parent/child");
            zkClient.createPersistent("/parent/child2","child2");
            for (int i=0;i<10;i++){
                zkClient.createEphemeralSequential("/parent/ephmerial_","ephmerial" +i);
            }
            //为了监听到回调，不至于主线程退出，这里手动调用线程休眠
            Thread.sleep(1000);
            //递归删除
            zkClient.deleteRecursive("/parent");
            System.out.println("-------------------------------------------------------------------------------");
            /**
             * subscribeDataChanges 订阅节点的内容变化
             */
            zkClient.createEphemeral("/father");
            CountDownLatch countDownLatch = new CountDownLatch(1);
            zkClient.subscribeDataChanges("/father", new IZkDataListener() {
                @Override
                public void handleDataChange(String dataPath, Object data) throws Exception {
                    //节点的内容改变事件
                    System.out.println("变更的节点为:" + dataPath + ", 变更内容为:" + data);
                }

                @Override
                public void handleDataDeleted(String dataPath) throws Exception {
                    //节点的删除事件
                    System.out.println("删除的节点为:" + dataPath);
                    countDownLatch.countDown();
                }
            });
            ZkClient finalZkClient = zkClient;
            //模拟另一个线程去删除节点
            new Thread(()->{
                try {
                    Thread.sleep(10000);
                    System.out.println("开始删除节点");
                    finalZkClient.delete("/father");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
            countDownLatch.await();
            System.out.println("节点已经删除，程序运行结束");
        }finally {
            if (zkClient != null){
                zkClient.close();
            }
        }
    }
}
