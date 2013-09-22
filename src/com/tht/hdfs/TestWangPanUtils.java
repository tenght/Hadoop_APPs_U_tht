package com.tht.hdfs;

import org.apache.hadoop.fs.Path;

public class TestWangPanUtils {
	
	public static void main(String[] args) throws Exception {
		String[] args1 = {"","hdfs://master:9000/in"};
		String[] args2 = {"","D:/test1.xml","hdfs://master:9000/in/tht.xml"};
//		String[] args3 = {"","hdfs://master:9000/in/tht.xml"};
		String[] args4 = {"","hdfs://master:9000/in/p","D:/test1.xml"};

		
		String uploadFile =WangPanUtils.uploadFile(args2);
		System.out.println(uploadFile);
//		String deleteFile =WangPanUtils.deleteFile(args3);
//		System.out.println(deleteFile);
		
		Path[] path=null;
		path=WangPanUtils.listFile(args1);
		for(Path pa : path){
		System.out.println(pa);
		}
		
		String readFile =WangPanUtils.readFile(args4);
		System.out.println(readFile);
	}
}
