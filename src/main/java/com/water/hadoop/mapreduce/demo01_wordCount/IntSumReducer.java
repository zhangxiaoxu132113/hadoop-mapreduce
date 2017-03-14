package com.water.hadoop.mapreduce.demo01_wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * Created by zhangmiaojie on 2017/3/14.
 */
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable result = new IntWritable(0);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
