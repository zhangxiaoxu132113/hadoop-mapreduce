package com.water.hadoop.mapreduce.demo03_baiduRankTop20;

import com.jk39.bdc.db.RankDetailVO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * 统计排名前20的网站的总数
 */
public class BaiduRankTop10Job {

	private final static String PATH_JAR = "E:\\personal\\practice\\hadoop-mr\\out\\artifacts\\top20Rank.jar";

	private final static String PATH_OUT_TMP = "baiduranktop20-temp-output-01";

	public static class RankMapper extends TableMapper<Text, IntWritable> {

		private static IntWritable one = new IntWritable(1);

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			for (int i = 1; i <= 20; i++) {
				byte[] buffer = value.getValue(Bytes.toBytes("DATA"), Bytes.toBytes(String.format("RANK_%s", i)));
				try {
					if (buffer != null) {
						RankDetailVO baiduVO = RankDetailVO.parseFromBytes(buffer);
						context.write(new Text(baiduVO.getDomain()), one);
					}
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class InverseMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
		@Override
		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			context.write(value, key);
		}
	}

	public static class RankReducer extends TableReducer<IntWritable, Text, NullWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				Put put = new Put(Bytes.toBytes(val.toString()));
				put.addImmutable(Bytes.toBytes("RESULT"), Bytes.toBytes("NUM"), Bytes.toBytes(String.valueOf(key.get())));
				context.write(NullWritable.get(), put);
			}
		}
	}

	public static class IntWritableDecreasingComparator extends IntWritable.Comparator {
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.default.name", "hdfs://hadooptest:9000");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.address", "hadooptest:8032");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "hadooptest");

		Path tempDir = new Path(PATH_OUT_TMP);

		Job sumJob = Job.getInstance(conf, "BaiduRankTop20-Sum");
		sumJob.setJar(PATH_JAR);

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("DATA"));
		TableMapReduceUtil.initTableMapperJob("T_PAGE_RANK_BAIDU", scan, RankMapper.class, Text.class, IntWritable.class, sumJob);

		sumJob.setCombinerClass(SumReducer.class);
		sumJob.setReducerClass(SumReducer.class);
		sumJob.setOutputKeyClass(Text.class);
		sumJob.setOutputValueClass(IntWritable.class);
		sumJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(sumJob, tempDir);

		sumJob.waitForCompletion(true);

		Job sortjob = Job.getInstance(conf, "BaiduRankTop20-Sort");
		sortjob.setJar(PATH_JAR);

		FileInputFormat.addInputPath(sortjob, tempDir);
		sortjob.setInputFormatClass(SequenceFileInputFormat.class);
		sortjob.setMapperClass(InverseMapper.class);
		sortjob.setMapOutputKeyClass(IntWritable.class);
		sortjob.setMapOutputValueClass(Text.class);
		sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);
		TableMapReduceUtil.initTableReducerJob("T_DOAMIN_TOP20_RESULT_BAIDU", RankReducer.class, sortjob);

		sortjob.waitForCompletion(true);

		FileSystem.get(conf).delete(tempDir, true);

		System.exit(0);
	}
}
