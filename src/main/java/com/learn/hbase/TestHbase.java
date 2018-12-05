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
    private static Admin admin = null;
    static{
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.37.100");
        /* 获取hbase的管理员对象 */
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void close(Connection conn,Admin admin){
        if(conn!=null){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

//    public static boolean old_tableExist(String tableName) throws IOException {
//        HBaseConfiguration configuration = new HBaseConfiguration();
//        configuration.set("hbase.zookeeper.quorum", "192.168.37.100");
//        //获取hbase的管理员对象
//        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
//        boolean tableExists = hBaseAdmin.tableExists(tableName);
//        hBaseAdmin.close();
//        return tableExists;
//    }

    private static boolean tableExist(String tableName) throws IOException {
//        HBaseConfiguration configuration = new HBaseConfiguration();

        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        return tableExists;
    }


    //创建表
    public void createTable(String tableName,String... cfs){


    }

    //删除表
    //CRUD
    public static void main(String[] args) throws IOException {
        System.out.println("123");
        System.out.println(tableExist("staff"));
        System.out.println(tableExist("student"));
    }
}
