package com.learn.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitRunner extends Configuration implements Tool {
    private Configuration configuration = null;

    @Override
    public int run(String[] strings) throws Exception {
        Job instance = Job.getInstance(configuration);
        //指定driver类，mapper，reducer
        instance.setJarByClass(FruitRunner.class);
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("fruit"),
                new Scan(),
                FruitMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                instance
        );
        TableMapReduceUtil.initTableReducerJob("fruit_mr",
                FruitReducer.class,
                instance);

        //submit
        boolean b = instance.waitForCompletion(true);
        return b ? 1 : 0;


    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int i = ToolRunner.run(configuration,new FruitRunner(),args);

    }
}
