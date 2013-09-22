package com.tht.hdfs;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileStatus;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
  
import java.io.IOException;  
import java.net.URI;  

public class ShowFileStatus {  
  
    public static void main(String[] args) throws IOException {  
        Path path = new Path("hdfs://121.1.253.251:9000/README.txt");  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create("hdfs://121.1.253.251:9000/"), conf);  
        FileStatus status = fs.getFileStatus(path);  
        System.out.println("path = " + status.getPath());  
        System.out.println("owner = " + status.getOwner());  
        System.out.println("block size = " + status.getBlockSize());  
        System.out.println("permission = " + status.getPermission());  
        System.out.println("replication = " + status.getReplication());  
    }  
}