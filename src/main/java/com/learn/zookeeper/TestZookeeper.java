package com.learn.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestZookeeper {
    private String connectString = "hadoop100:2181,hadoop101:2181,hadoop102:2181";//链接的zk集群

    private int sessionTimeOut = 2000;
    private ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
//                System.out.println("--------------------------");
//                List<String> children = null;
//                try {
//                    children = zkClient.getChildren("/",true);
//                    for (String child:children){
//                        System.out.println(child);
//
//                    }
//                    System.out.println("--------------------------");
//                } catch (KeeperException e) {
//                    e.printStackTrace();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

            }
        });
    }
    //创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String path = zkClient.create("/atguigu","helloworld".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }
    //2 获取子节点 并监控数据变化
    @Test
    public  void getDataAndWatch() throws KeeperException, InterruptedException {

        List<String> children =zkClient.getChildren("/",true);
        for (String child:children){
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }
    //判断节点是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {
       Stat stat = zkClient.exists("/atguigu",false);
        System.out.println(stat==null?"Not exist":"exist");

    }
}
