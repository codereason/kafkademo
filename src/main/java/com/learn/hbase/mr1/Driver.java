package com.learn.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configuration implements Tool {
    private Configuration configuration = null;

    @Override
    public int run(String[] args) throws Exception {
        Job instance = Job.getInstance(configuration);
        //指定driver类，mapper，reducer
        instance.setJarByClass(Driver.class);
        instance.setMapperClass(HDFStoHbaseMapper.class);
        instance.setMapOutputKeyClass(NullWritable.class);
        instance.setMapOutputValueClass(Put.class);
        TableMapReduceUtil.initTableReducerJob("fruit_2",
                HDFStoHbaseReducer.class,
                instance);
//设置输入路径
        FileInputFormat.setInputPaths(instance, new Path(args[0]));
        //submit
        boolean b = instance.waitForCompletion(true);
        return b ? 1 : 0;
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int i = ToolRunner.run(configuration, new Driver(), args);
        System.exit(i);


    }
}
