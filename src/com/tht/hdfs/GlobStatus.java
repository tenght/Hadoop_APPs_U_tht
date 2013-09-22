package com.tht.hdfs;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileStatus;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.FileUtil;  
import org.apache.hadoop.fs.Path;  
  
import java.io.IOException;  
import java.net.URI;  

public class GlobStatus {  
    public static void main(String[] args) throws IOException {  
        String uri = "hdfs://121.1.253.251:9000/in/*";  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(uri), conf);  
  
        FileStatus[] status = fs.globStatus(new Path(uri),new RegexExcludePathFilter("^.*/"));  
        Path[] listedPaths = FileUtil.stat2Paths(status);  
        for (Path p : listedPaths) {  
            System.out.println(p);  
        }  
    }  
} 
