package com.water.hadoop.mapreduce.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 *
 * Created by zhangmiaojie on 2017/3/10.
 */
public class HBaseUtils {

    private static Configuration conf;

    static {
        conf = HBaseConfiguration.create(); //初始化HBase的配置文件
        InputStream input = HBaseUtils.class.getResourceAsStream("/core-site.xml");
        conf.addResource(input);
    }

    public static Configuration getConfiguration() {
        return conf;
    }

    public static Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection();
    }

    /**
     * 实例化HBaseAdmin, HBaseAdmin 用于对表的元素据进行操作
     *
     * @return
     * @throws java.io.IOException
     */
    public static Admin getAdmin() throws IOException {
        return getConnection().getAdmin();
    }

    public static void createTable(String tableName, String... columnFamilies) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tn);
        for (String col : columnFamilies) {
            tableDescriptor.addFamily(new HColumnDescriptor(col));
        }
        if (getAdmin().isTableDisabled(tn)) {
            getAdmin().enableTable(tn);
        }
        if (getAdmin().tableExists(tn)) {
            getAdmin().disableTable(tn);
            System.out.println("table is exist....");
            getAdmin().deleteTable(tn);
            System.out.println("table delted! Dnoe...");
        }
        getAdmin().createTable(tableDescriptor);
    }

    /**
     * 获取表
     *
     * @param tableName 表名
     * @return
     * @throws java.io.IOException
     */
    public static HTable getHTable(String tableName) throws IOException {
        return new HTable(conf, tableName);
    }

    /**
     * 获取Put,Put是插入一行数据的封装格式
     *
     * @param rowKey       行key值
     * @param columnFamily 列簇
     * @param qualifier
     * @param value        列值
     * @return Put
     * @throws java.io.IOException
     */
    public static Put getPut(String rowKey, String columnFamily, String qualifier, String value) throws IOException {
        Put put = new Put(rowKey.getBytes());
        if (qualifier == null || "".equals(qualifier)) {
            put.add(columnFamily.getBytes(), null, value.getBytes());
        } else {
            put.add(columnFamily.getBytes(), qualifier.getBytes(), value.getBytes());
        }
        return put;
    }

    /**
     * 查询某一行的数据
     * @param tableName  表名
     * @param rowKey     行键
     * @return
     * @throws java.io.IOException
     */
    public static Result getResult(String tableName,String rowKey) throws IOException {
        Get get = new Get(rowKey.getBytes());
        HTable htable  = getHTable(tableName);
        Result result = htable.get(get);
        htable.close();
        return result;

    }

    /**
     * 条件查询
     * @param tableName         表名
     * @param columnFamily      列族
     * @param queryCondition    查询条件值
     * @param begin             查询的起始行
     * @param end               查询的终止行
     * @return
     * @throws java.io.IOException
     */
    public static ResultScanner getResultScanner(String tableName,String columnFamily,String queryCondition,String begin,String end) throws IOException{
        Scan scan = new Scan();
        //设置起始行
        scan.setStartRow(Bytes.toBytes(begin));
        //设置终止行
        scan.setStopRow(Bytes.toBytes(end));

        //指定要查询的列族
        scan.addColumn(Bytes.toBytes(columnFamily),null);
        //查询列族中值等于queryCondition的记录
        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),null, CompareFilter.CompareOp.EQUAL,Bytes.toBytes(queryCondition));
        //Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),null,CompareOp.EQUAL,Bytes.toBytes("chuliuxiang"));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, Arrays.asList(filter1));

        scan.setFilter(filterList);
        HTable htable  = getHTable(tableName);

        ResultScanner rs = htable.getScanner(scan);
        htable.close();
        return rs;
    }


}

