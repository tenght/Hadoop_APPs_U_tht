package com.tht.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public class TestWangPanUtils2 {
    
	/*** 运行结果描述. */
    private static String result = "";
 
    public static void main(String[] args1) {
    	
//    	String[] argsUpload = {"upload","D:/test1.xml","hdfs://master:9000/in/tht.xml"};
    	String[] args = {"query","hdfs://master:9000/in/","D:/test1.xml"};
    	
        try {
            // 判断命令输入是否正确
            if (args[0] != null && !"".equals(args[0]) && args.length > 0) {
                if ("upload".equals(args[0])) {
                    result = "upload:" + WangPanUtils.uploadFile(args);
                } else if ("delete".equals(args[0])) {
                    result = "delete:" + WangPanUtils.deleteFile(args);
                } else if ("query".equals(args[0])) {
                    if (WangPanUtils.listFile(args) == null) {
                        result = "query:fail!";
                    } else {
                        result = "query:success";
                        Path[] path = WangPanUtils.listFile(args);
                		for(Path pa : path){
                		System.out.println(pa);
                		}
                    }
                } else if ("read".equals(args[0])) {
                    result = "read:" + WangPanUtils.readFile(args);
                } else {
                    System.out.println("sorry,wo have no this service!");
                }
                System.out.println(result);
            } else {
                System.out.println("fail!");
                System.exit(1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
