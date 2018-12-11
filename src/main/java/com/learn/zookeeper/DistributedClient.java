package com.learn.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributedClient {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //获取zookeeper集群连接
        DistributedClient client = new DistributedClient();
        client.getConnect();
        //注册监听
        client.getChildren();

        //业务逻辑
        client.business();
    }
    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);

    }
    private void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/servers", true);
        //存储服务器节点主机名称集合
        ArrayList<String> hosts = new ArrayList<String>();

        for (String child : children) {
            byte[] data = zkClient.getData("/servers/" + child, false, null);
            hosts.add(new String(data));
        }
        //将所有在线主机名称打印
        System.out.println(hosts);
    }


    private String connectString = "hadoop100:2181,hadoop101:2181,hadoop102:2181";//链接的zk集群

    private int sessionTimeOut = 2000;
    private ZooKeeper zkClient;


    private void getConnect() throws IOException {

        zkClient = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {

            public void process(WatchedEvent watchedEvent) {
                try {
                    getChildren();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}

