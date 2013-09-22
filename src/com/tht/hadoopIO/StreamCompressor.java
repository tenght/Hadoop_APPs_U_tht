package com.tht.hadoopIO;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

//vv StreamCompressor
public class StreamCompressor {

	public static void main(String[] args) throws Exception {
		String codecClassname = "org.apache.hadoop.io.compress.GzipCodec";
		String uri = "hdfs://master:9000/in/test.txt";
		String outputUri = "hdfs://master:9000/in/test.txt.gz";
		
		Class<?> codecClass = Class.forName(codecClassname);
		Configuration conf = new Configuration();
		FileSystem fs1 = FileSystem.get(URI.create(uri), conf);
		FileSystem fs2 = FileSystem.get(URI.create(outputUri), conf);
		
		CompressionCodec codec = (CompressionCodec) ReflectionUtils
				.newInstance(codecClass, conf);

		InputStream in =fs1.open(new Path(uri));	
		CompressionOutputStream out = codec.createOutputStream(fs2.create(new Path(outputUri)));
		
		IOUtils.copyBytes(in, out, 4096, false);
		in.close();
		out.close();
	}
}
// ^^ StreamCompressor