package com.water.hadoop.mapreduce.demo02_wordCount_hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by zhangmiaojie on 2017/3/14.
 */
public class MyReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable> {


    protected void reduce(Text key, Iterable<IntWritable> values, Reducer.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val:values) {
            sum+=val.get();
        }
        //添加一行记录，每一个单词作为行键
        Put put = new Put(Bytes.toBytes(key.toString()));
        //在列族result中添加一个标识符num,赋值为每个单词出现的次数
        //String.valueOf(sum)先将数字转化为字符串，否则存到数据库后会变成\x00\x00\x00\x这种形式
        //然后再转二进制存到hbase。
        put.add(Bytes.toBytes("result"), Bytes.toBytes("num"), Bytes.toBytes(String.valueOf(sum)));
        context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
    }
}
