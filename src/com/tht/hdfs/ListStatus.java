package com.tht.hdfs;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileStatus;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.FileUtil;  
import org.apache.hadoop.fs.Path;  
  
import java.io.IOException;  
import java.net.URI;  

public class ListStatus {
    public static void main(String[] args) throws IOException {  
        //String uri = args[0];
    	String uri = args[0];
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path[] paths = new Path[args.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path(args[i]);
        }  
  
        FileStatus[] status = fs.listStatus(paths);
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for (Path p : listedPaths) {
            System.out.println(p);
        }
    }
}
//导出jar包，然后在部署了hadoop的机器上运行成功。eg:
//hadoop jar listStatus.jar com.tht.hdfs.ListStatus  hdfs://master:9000/in