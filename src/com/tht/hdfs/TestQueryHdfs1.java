package com.tht.hdfs;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//使用 Java 遍历 Ubuntu 中 HDFS 上的目录和文件
public class TestQueryHdfs1 {
	private static FileSystem hdfs;

	public static void main(String[] args) throws Exception {
		// 1.创建配置器
		Configuration conf = new Configuration();
		// 2.创建文件系统（手动指定 HDFS的 URI）
		hdfs = FileSystem.get(URI.create("hdfs://master:9000/"), conf);
		// 3.遍历HDFS上的文件和目录
		FileStatus[] fs = hdfs.listStatus(new Path("/"));
		if (fs.length > 0) {
			for (FileStatus f : fs) {
				showDir(f);
			}
		}
	}

	private static void showDir(FileStatus fs) throws Exception {
		Path path = fs.getPath();
		System.out.println(path);
		// 如果是目录
		if (fs.isDir()) {
			FileStatus[] f = hdfs.listStatus(path);
			if (f.length > 0) {
				for (FileStatus file : f) {
					showDir(file);
				}
			}
		}
	}
}