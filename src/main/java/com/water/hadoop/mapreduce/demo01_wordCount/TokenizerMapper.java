package com.water.hadoop.mapreduce.demo01_wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 *
 * Created by zhangmiaojie on 2017/3/14.
 */
public class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable> {
    IntWritable one = new IntWritable(1);
    Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        while (stringTokenizer.hasMoreTokens()) {
            word.set(stringTokenizer.nextToken());
            context.write(word,one);
        }
    }
}
