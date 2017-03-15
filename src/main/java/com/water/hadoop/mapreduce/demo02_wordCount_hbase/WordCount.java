package com.water.hadoop.mapreduce.demo02_wordCount_hbase;


import com.water.hadoop.mapreduce.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by zhangmiaojie on 2017/3/14.
 */
public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(HBaseUtils.getConfiguration(),"wordstat");
        job.setJarByClass(WordCount.class);

        Scan scan = new Scan();
        //指定要查询的列族
        scan.addColumn(Bytes.toBytes("content"),null);
        //指定Mapper读取的表为word
        TableMapReduceUtil.initTableMapperJob("word", scan, MyMapper.class, Text.class, IntWritable.class, job);
        //指定Reducer写入的表为stat
        TableMapReduceUtil.initTableReducerJob("stat", MyReducer.class, job);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
