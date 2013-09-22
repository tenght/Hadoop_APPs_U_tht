package com.tht.hdfs;

import java.io.IOException;
import java.util.StringTokenizer; //分词器
import org.apache.hadoop.conf.Configuration; //配置器
import org.apache.hadoop.fs.Path; //路径
import org.apache.hadoop.io.IntWritable; //整型写手
import org.apache.hadoop.io.Text; //文本写手
import org.apache.hadoop.mapreduce.Job; //工头
import org.apache.hadoop.mapreduce.Mapper; //映射器
import org.apache.hadoop.mapreduce.Reducer; //拆分器
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; //文件格式化读取器
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; //文件格式化创建器

public class WordCount {
	/**
	 * 内部类：映射器 Mapper<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT>
	 */
	public static class MyMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);// 类似于int类型
		private Text word = new Text(); // 可以理解成String类型

		/*** 重写map方法 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			System.err.println(key + "," + value);
			// 分词器：默认根据空格拆分字符串
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		};
	}

	/**
	 * 内部类：拆分器 Reducer<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT>
	 */
	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		/**
		 * 重写reduce方法
		 */
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			System.err.println(key + "," + values);
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);// 这是最后结果
		};
	}

	public static void main(String[] args) throws Exception {
		// 声明配置信息
		Configuration conf = new Configuration();
		conf.addResource(new Path("d:/core-site.xml"));
		//FileSystem.get(URI.create("hdfs://192.168.203.138:9000/"), conf);

		// 声明Job
		Job job = new Job(conf, "Word Count");
		// 设置工作类
		job.setJarByClass(WordCount.class);// 设置mapper类
		job.setMapperClass(MyMapper.class);
		// 可选
		job.setCombinerClass(MyReducer.class);
		// 设置合并计算类
		job.setReducerClass(MyReducer.class);
		// 设置key为String类型
		job.setOutputKeyClass(Text.class);
		// 设置value为int类型
		job.setOutputValueClass(IntWritable.class);
		// 设置接收输入或是输出
		FileInputFormat.setInputPaths(job, new Path("hdfs://121.1.253.251:9000/README.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://121.1.253.251:9000/out"));
		// 执行
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}