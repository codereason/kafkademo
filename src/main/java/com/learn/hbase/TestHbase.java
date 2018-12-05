package com.learn.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class TestHbase {
    //判断表是否存在


    public static boolean old_tableExist(String tableName) throws IOException {
        HBaseConfiguration configuration = new HBaseConfiguration();
        configuration.set("hbase.zookeeper.quorum", "192.168.37.100");
        //获取hbase的管理员对象
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        boolean tableExists = hBaseAdmin.tableExists(tableName);
        hBaseAdmin.close();
        return tableExists;
    }

    private static boolean tableExist(String tableName) throws IOException {
//        HBaseConfiguration configuration = new HBaseConfiguration();
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.37.100");
        /* 获取hbase的管理员对象 */
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        return tableExists;
    }


    //创建表


    //删除表
    //CRUD
    public static void main(String[] args) throws IOException {
        System.out.println("123");
        System.out.println(tableExist("staff"));
        System.out.println(tableExist("student"));
    }
}
