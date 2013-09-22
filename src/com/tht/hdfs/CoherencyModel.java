package com.tht.hdfs;

import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CoherencyModel {
	public static void main(String[] args) throws Exception {  
	String uri = "hdfs://121.1.253.251:9000/in/";
    Configuration conf=new Configuration();  
    FileSystem fs=FileSystem.get(URI.create(uri),conf);
    Path p = new Path(uri+"/p");//如果改为Path p = new Path("/p");则输出结果变为../user/hadoop/p
    OutputStream out = fs.create(p);
    out.write("content for tht test".getBytes("UTF-8"));
    out.flush();
    out.close();
    System.out.println(fs.getFileStatus(p).getPath()); 
    }
}