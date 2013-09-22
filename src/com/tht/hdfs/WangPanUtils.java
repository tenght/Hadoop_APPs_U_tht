package com.tht.hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
 
public class WangPanUtils {
 
    /**
     * 上传文件. java -jar /root/hdfs-0.0.1-SNAPSHOT.jar upload /root/test-hdfs.txt
     * hdfs://hadoopm:9000/user/root/upload/12390po.txt
     *
     * @param args
     * @return
     * @throws IOException
     */
    public static String uploadFile(String[] args) throws IOException {
 
        String loaclSrc = args[1];
        String dst = args[2];
        if (args.length < 3) {
            return "fail";
        }
 
        InputStream in = new BufferedInputStream(new FileInputStream(loaclSrc));
 
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
 
        OutputStream out = fs.create(new Path(dst));
 
        IOUtils.copyBytes(in, out, 4096, true);
 
        return "success";
    }
 
    /**
     * 查询文件列表. java -jar /root/hdfs-0.0.1-SNAPSHOT.jar query
     * hdfs://hadoopm:9000/user/root/
     *
     * @param args
     * @return
     * @throws IOException
     */
    public static Path[] listFile(String[] args) throws IOException {
        if (args.length < 2) {
            return null;
        }
 
        String dst = args[1];
 
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FileStatus[] statu = fs.listStatus(new Path(dst));
        Path[] listPaths = FileUtil.stat2Paths(statu);
        return listPaths;
    }
 
    /**
     * 删除文件.
     * java -jar /root/hdfs-0.0.1-SNAPSHOT.jar delete hdfs://hadoopm:9000/user/root/upload/12390po.txt
     *
     * @param args
     * @return
     * @throws IOException
     */
    public static String deleteFile(String[] args) throws IOException {
        if (args.length < 2) {
            return "fail";
        }
 
        String fileName = args[1];
 
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(fileName), config);
        Path path = new Path(fileName);
 
        if (!hdfs.exists(path)) {
            return "fail";
        }
 
        boolean isDeleted = hdfs.delete(path, false);
        if (isDeleted) {
            return "success";
        } else {
            return "fail";
        }
 
    }
 
    /**
     * 读取文件.
     * java -jar /root/hdfs-0.0.1-SNAPSHOT.jar read hdfs://hadoopm:9000/user/root/upload/123.txt /root/test-readfile898.txt
     *
     * @param args
     * @return
     * @throws IOException
     */
    public static String readFile(String[] args) throws IOException {
 
        if(args.length < 3){
            return "fail";
        }
 
        String dst = args[1];
        String newPath = args[2];
 
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FSDataInputStream hdfsInStream = fs.open(new Path(dst));
 
        OutputStream out = new FileOutputStream(newPath);
        byte[] ioBuffer = new byte[1024];
        int readLen = hdfsInStream.read(ioBuffer);
        while (-1 != readLen) {
            out.write(ioBuffer, 0, readLen);
            readLen = hdfsInStream.read(ioBuffer);
        }
        out.close();
        hdfsInStream.close();
        fs.close();
        return "success";
    }
 
 
/**
     * 创建文件夹.
     * java -jar /root/hdfs-0.0.1-SNAPSHOT.jar mkdir hdfs://hadoopm:9000/user/root/upload/test909
     *
     * @return
     * @throws IOException
     */
    public static String mkdir(String[] args) throws IOException{
 
        if(args.length < 2){
            return "fail";
        }
 
        String dst = args[1];
 
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        Path path = new Path(dst);
 
        if (fs.exists(path)) {
            return "fail";
        }
 
        fs.mkdirs(path);
 
        return "success";
    }
}