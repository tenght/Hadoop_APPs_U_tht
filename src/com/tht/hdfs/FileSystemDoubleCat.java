package com.tht.hdfs;

//cc FileSystemDoubleCat Displays files from a Hadoop filesystem on standard output twice, by using seek
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

//vv FileSystemDoubleCat
public class FileSystemDoubleCat {

	public static void main(String[] args) throws Exception {
		// String uri = args[0];
		String uri = "hdfs://121.1.253.251:9000/in/core-site.xml";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
        byte b[] = new byte[500];
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			//in.seek(0); // go back to the start of the file
			//IOUtils.copyBytes(in, System.out, 4096, false);
			
			in.read(83,b,10,300);
			System.out.println(new String(b)); 
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
// ^^ FileSystemDoubleCat