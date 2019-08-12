package org.nmq.zookeeper.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author niemengquan
 * @create 2019/8/8
 * @modifyUser
 * @modifyDate
 */
public class ZookeeperTest  implements Watcher{

    private ZooKeeper zooKeeper;

    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final int SESSION_TIME_OUT = 2000;


    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("调用了监听程序："+ watchedEvent.getPath());
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected){
            if(watchedEvent.getType() == Event.EventType.None && null == watchedEvent.getPath()) {
                // 最初与zk服务器建立好连接
                System.out.println("连接建立事件！");
                countDownLatch.countDown();
            } else if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                // 子节点变化事件
                System.out.println("子节点改变事件"+ watchedEvent.getPath());
            } else if(watchedEvent.getType() == Event.EventType.NodeDeleted) {
                // 节点被删除
                System.out.println("删除事件"+watchedEvent.getPath());
            }
        }
        if (watchedEvent.getPath() != null){
            try {
                //因为监听是一次性的，为了持续监听需要重新设置监听
                this.getChildren(watchedEvent.getPath(),true);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 建立zookeeper连接
     * @param zkAddress
     * @throws Exception
     */
    public void connectZK(String zkAddress) throws Exception {
        zooKeeper = new ZooKeeper(zkAddress,SESSION_TIME_OUT,this );
        //阻塞等待真正的建立连接
        countDownLatch.await();
        System.out.println("zookeeper connection success!");
    }

    /**
     * 同步创建节点
     *
     * @param path
     * @param data
     * @param createMode
     * createMode 有多重创建模式
     * <ul>
     *     <li>PERSISTENT:持久化的节点，连接关闭的时候不会删除节点</li>
     *     <li>PERSISTENT_SEQUENTIAL：持久化的连续节点，节点不会再连接关闭的时候自动删除，并且节点的名字将会自动追加一个递增的数字。例如如果
     *     你创建的节点是/root/child_,那么实际生成的时候是/root/child_1、/root/child_2等等.
     *                |root|
     *              /     \    \
     *           /        \       \
     *     |child_1|  |child_2|   |child_3|
     *     </li>
     *     <li>EPHEMERAL:临时节点。连接关闭之后将会被删除</li>
     *     <li>EPHEMERAL_SEQUENTIAL：临时的连续节点。连接关闭之后将会被删除，并且节点的名字将会自动追加一个递增的数字，同<PERSISTENT_SEQUENTIAL/li>
     * </ul>
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String createNodeSync(String path,byte[] data,CreateMode createMode) throws KeeperException, InterruptedException {
        return this.zooKeeper.create(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
    }

    /**
     * 如果给定的版本信息和path节点的版本一致，则删除指定path的节点信息。
     * @param path
     * @param version 加点的版本，-1 则是删除所有版本
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void deleteNode(String path,int version) throws KeeperException, InterruptedException {
        this.zooKeeper.delete(path,version);
    }

    /**
     * 删除节点信息
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void deleteNode(String path) throws KeeperException, InterruptedException {
        this.deleteNode(path,-1);
    }

    /**
     * 获取path节点下的孩子节点列表
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getChildren(String path,boolean watch) throws KeeperException, InterruptedException {
        /**
         * watch 属性标识是否要对path节点做watch：
         * false：不监听
         * true:监听该path节点。如果该path被删除，或者该path节点下的子节点被创建或者删除的时候将会触发事件监听器。
         */
        List<String> children = this.zooKeeper.getChildren(path, watch);
        return children;
    }

    /**
     * 获取path节点的内容
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String getData(String path) throws KeeperException, InterruptedException {
        byte[] data = this.zooKeeper.getData(path, false, null);
        return data == null? "" :new String(data);
    }

    /**
     * 更新节点信息，如果设置成功，将会通知所有通过getData API 获取watch的所有通知。
     * @param path
     * @param data
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Stat setData(String path,String data) throws KeeperException, InterruptedException {
        Stat stat = this.zooKeeper.setData(path, data.getBytes(), -1);
        return stat;
    }

    /**
     * 判断节点是否存在
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Stat exists(String path) throws KeeperException, InterruptedException {
        Stat stat = this.zooKeeper.exists(path, false);
        return stat;
    }

    /**
     * 关闭zk连接
     * @throws InterruptedException
     */
    public void closeConnection() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    public static void main(String[] args) throws Exception{
        ZookeeperTest zookeeperTest = new ZookeeperTest();
        //zookeeperTest.connectZK("192.168.154.130:2181");
        zookeeperTest.connectZK("127.0.0.1:2181");
        String nodeSync = zookeeperTest.createNodeSync("/LOCK/app_", "1".getBytes(),CreateMode.EPHEMERAL_SEQUENTIAL);
        String nodeSync1 = zookeeperTest.createNodeSync("/LOCK/app_", "1".getBytes(),CreateMode.EPHEMERAL_SEQUENTIAL);
        String nodeSync2 = zookeeperTest.createNodeSync("/LOCK/app_", "1".getBytes(),CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(nodeSync);
        System.out.println(nodeSync1);
        System.out.println(nodeSync2);
        List<String> children = zookeeperTest.getChildren("/LOCK", true);
        Collections.sort(children);
        children.forEach(node -> {
            try {
                System.out.println(node + ":" + zookeeperTest.getData("/LOCK/"+node));
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        //删除临时节点测试watch
        Iterator<String> iterator = children.iterator();
        while (iterator.hasNext()){
            String node = iterator.next();
            //zookeeperTest.deleteNode("/LOCK/"+ node);
            //System.out.println("删除节点：" + "/LOCK/" + node);
            zookeeperTest.setData("/LOCK/"+ node,"1");
            System.out.println("更新节点：" + "/LOCK/" + node);
        }
        System.out.println("--------------------------------------");
        List<String> rootChild = zookeeperTest.getChildren("/",false);
        rootChild.forEach(root -> System.out.println(root));
        zookeeperTest.closeConnection();
    }
}
