package com.tht.hadoopIO;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NewMaxTemperatureWithCombiner {
	  public static void main(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.err.println("Usage: MaxTemperatureWithCombiner <input path> " +
	          "<output path>");
	      System.exit(-1);
	    }
	    
	    Job job = new Job();
	    job.setJarByClass(MaxTemperatureWithCombiner.class);
	    job.setJobName("Max temperature");
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(NewMaxTemperatureMapper.class);
	    job.setCombinerClass(NewMaxTemperatureReducer.class);
	    job.setReducerClass(NewMaxTemperatureReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}