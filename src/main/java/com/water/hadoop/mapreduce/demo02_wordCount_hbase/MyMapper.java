package com.water.hadoop.mapreduce.demo02_wordCount_hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by zhangmiaojie on 2017/3/14.
 */
public class MyMapper extends TableMapper<Text, IntWritable> {

    private static IntWritable one = new IntWritable(1);
    private static Text word = new Text();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper.Context context) throws IOException, InterruptedException {
        //表里面只有一个列族，所以我就直接获取每一行的值
        String words = Bytes.toString(value.list().get(0).getValue());
        StringTokenizer st = new StringTokenizer(words);
        while (st.hasMoreTokens()) {
            String s = st.nextToken();
            word.set(s);
            context.write(word, one);
        }
    }
}
