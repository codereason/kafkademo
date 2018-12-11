package com.learn.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
//服务器代码，注册，业务等
//
public class DistributedServer {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //连接zookeeper集群 并注册

        DistributedServer server = new DistributedServer();
        server.getConnect();
        server.regist(args[0]); //注册就是创建一个节点


        //业务逻辑处理
        server.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);

    }

    private void regist(String hostname) throws KeeperException, InterruptedException {
        zkClient.create("/servers/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);//服务器是暂时的 此外服务器不能重复 所以选择EPHEMERAL_SEQUENTIAL
        System.out.println(hostname + " is online");
    }

    private String connectString = "hadoop100:2181,hadoop101:2181,hadoop102:2181";//链接的zk集群

    private int sessionTimeOut = 2000;
    private ZooKeeper zkClient;


    private void getConnect() throws IOException {

        zkClient = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {

            public void process(WatchedEvent watchedEvent) {

            }
        });

    }
}
