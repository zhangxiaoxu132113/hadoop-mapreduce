package com.water.hadoop.mapreduce.demo02_wordCount_hbase;


import com.water.hadoop.mapreduce.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangmiaojie on 2017/3/14.
 */
public class InitData {
    public static void main(String[] args) throws IOException {
        HBaseUtils.createTable("word", "content");

        HTable htable = HBaseUtils.getHTable("word");
        htable.setAutoFlush(false);

        List<Put> puts = new ArrayList<Put>();
        Put put1 = HBaseUtils.getPut("1","content",null,"The Apache Hadoop software library is a framework");
        Put put2 = HBaseUtils.getPut("2","content",null,"The common utilities that support the other Hadoop modules");
        Put put3 = HBaseUtils.getPut("3","content",null,"Hadoop by reading the documentation");
        Put put4 = HBaseUtils.getPut("4","content",null,"Hadoop from the release page");
        Put put5 = HBaseUtils.getPut("5","content",null,"Hadoop on the mailing list");

        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        puts.add(put5);

        //提交测试数据
        htable.put(puts);
        htable.flushCommits();
        htable.close();
        //创建stat表，只有一个列祖result
        HBaseUtils.createTable("stat","result");
    }
}
