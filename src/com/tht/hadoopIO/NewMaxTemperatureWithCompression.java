package com.tht.hadoopIO;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NewMaxTemperatureWithCompression {
    public static void main(String[] args) throws Exception {
//        if (args.length!=2){
//            System.out.println("Usage: MaxTemperature <input path> <out path>");
//            System.exit(-1);
//        }
    	
    	String uri = "hdfs://master:9000/in/ncdc/sample.txt.gz";
		String outputUri = "hdfs://master:9000/output";
    	
        Job job=new Job();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max Temperature");

        FileInputFormat.addInputPath(job, new Path(uri));
        FileOutputFormat.setOutputPath(job, new Path(outputUri));

        job.setMapperClass(NewMaxTemperatureMapper.class);
        job.setCombinerClass(NewMaxTemperatureReducer.class);
        job.setReducerClass(NewMaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
