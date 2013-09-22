package com.tht.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//判断 HDFS 中指定名称的目录或文件：
public class TestQueryHdfs3 {
	private static FileSystem hdfs;

	public static void main(String[] args) throws Exception {
		// 1.配置器
		Configuration conf = new Configuration();
		// conf.addResource(new Path("c:/core-site.xml"));
		// 2.文件系统
		//hdfs = FileSystem.get(conf);
		
		hdfs = FileSystem.get(URI.create("hdfs://192.168.203.138:9000/"), conf);
		// 3.遍历HDFS目录和文件
		FileStatus[] fs = hdfs.listStatus(new Path("/"));
		if (fs.length > 0) {
			for (FileStatus f : fs) {
				showDir(f);
			}
		}
	}

	private static void showDir(FileStatus fs) throws Exception {
		Path path = fs.getPath();
		// 如果是目录
		if (fs.isDir()) {
			if (path.getName().equals("system")) {
				System.out.println(path + "是目录");
			}
			FileStatus[] f = hdfs.listStatus(path);
			if (f.length > 0) {
				for (FileStatus file : f) {
					showDir(file);
				}
			}
		} else {
			if (path.getName().equals("test.txt")) {
				System.out.println(path + "是文件");
			}
		}
	}
}
