package com.learn.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestHbase {
    //判断表是否存在
    private static Admin admin = null;
    private static Connection connection = null;
    private static Configuration configuration = null;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.37.100");
        /* 获取hbase的管理员对象 */

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void close(Connection conn, Admin admin) {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (admin != null) {
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
    public static void createTable(String tableName, String... cfs) throws IOException {//
        if (tableExist(tableName)) {
            System.out.println("Table already exists");
            return;
        }
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //add column family
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
//            hColumnDescriptor.setMaxVersions();
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(hTableDescriptor);
        System.out.println("Create table success");
    }

    //删除表
    public static void deleteTable(String tableName) throws IOException {
        if (tableExist(tableName)) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));

        } else {
            System.out.println("No such table");
            return;
        }

        if (!tableExist(tableName)) {
            System.out.println("Delete table " + tableName + " success");
        }
    }


    //CRUD
    public static void putData(String tableName, String rowkey, String cf, String cn, String value) throws IOException {
        Table table= connection.getTable(TableName.valueOf(tableName));


        //创建put对象
        Put put = new Put(Bytes.toBytes(rowkey));
        //添加数据
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        //执行添加操作
        table.put(put);
    }

    public static void main(String[] args) throws IOException {
        System.out.println("123");
//        System.out.println(tableExist("staff"));
//        System.out.println(tableExist("student"));

//        deleteTable("clienttable");
//        System.out.println(tableExist("clienttable"));
        putData("student","1003","info","name","huangyu");
        close(connection, admin);

    }
}
