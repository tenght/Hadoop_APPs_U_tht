package com.tht.hdfs;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IOUtils;  
import org.apache.hadoop.util.Progressable;  
  
import java.io.BufferedInputStream;  
import java.io.FileInputStream;  
import java.io.InputStream;  
import java.io.OutputStream;  
import java.net.URI;  

public class FileCopyWithProgress {  
    public static void main(String[] args) throws Exception {  
        String localSrc = "D:/core-site.xml";  
        String dst = "hdfs://121.1.253.251:9000/out/core-site.xml";  

        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));  
  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(dst), conf);  
        OutputStream out = fs.create(new Path(dst), new Progressable() {  
            @Override  
            public void progress() {  
                System.out.print(".");  
            }  
        });  
  
        IOUtils.copyBytes(in, out, 4096, true);  
        /*copyBytes
		public static void copyBytes(InputStream in,
		                             OutputStream out,
		                             int buffSize,
		                             boolean close)
		                      throws IOExceptionCopies from one stream to another. 
		
		Parameters:
		in - InputStrem to read from
		out - OutputStream to write to
		buffSize - the size of the buffer
		close - whether or not close the InputStream and OutputStream at the end. The streams are closed in the finally clause. 
		Throws: 
		IOException
         * */
    }
}
