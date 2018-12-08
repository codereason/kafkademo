package com.learn.hbase.mr1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//将hhdfs的数据读到hbase表里
//
public class HDFStoHbaseMapper extends Mapper<LongWritable,Text,NullWritable,Put>{

}
