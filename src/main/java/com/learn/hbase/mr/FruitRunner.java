package com.learn.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class FruitRunner extends Configuration implements Tool {
    private Configuration configuration = null;

    @Override
    public int run(String[] strings) throws Exception {
        Job instance = Job.getInstance(configuration);
        //指定driver类，mapper，reducer
        instance.setJarByClass(FruitRunner.class);
        TableMapReduceUtil.initTableMapperJob();
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }
}
