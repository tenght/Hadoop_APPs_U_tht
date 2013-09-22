package com.tht.test;

//使用InputStream从文件中读取数据，在已知文件大小的情况下，建立合适的存储字节数组  
import java.io.File;  
import java.io.InputStream;  
import java.io.FileInputStream;  
public class InputStreamDemo01  
{  
  public static void main(String args[])throws Exception{  
      File f = new File("D:/core-site.xml");  
      InputStream in = new FileInputStream(f);  
      byte b[]=new byte[(int)f.length()];     //创建合适文件大小的数组  
      in.read(b);    //读取文件中的内容到b[]数组  
      in.close();  
      System.out.println(new String(b));  
  }  
} 