package com.tht.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;  
import org.apache.hadoop.io.IOUtils;  
  
import java.io.InputStream;  
import java.net.URL;  

public class URLCat {  
  
    static {  
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());  
    }  
  
    public static void main(String[] args) throws Exception {  
        InputStream in = null;  
        try {  
            in = new URL("hdfs://master:9000/in/test.txt").openStream();  
            IOUtils.copyBytes(in, System.out, 4096, false);
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
        } finally {  
            IOUtils.closeStream(in);  
        }
    }  
}