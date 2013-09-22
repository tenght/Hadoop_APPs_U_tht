package com.tht.hadoopIO;

import junit.framework.Assert;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class TestWritable {
	byte[] bytes = null;

	/**
	 * 初始化一个IntWritable实例，并且调用序列化方法
	 * 
	 * @throws IOException
	 */
	@Before
	public void init() throws IOException {
		IntWritable writable = new IntWritable(163);
		bytes = serialize(writable);
	}

	/**
	 * 一个IntWritable序列号后的四个字节的字节流 并且使用big-endian的队列排列
	 * 
	 * @throws IOException
	 */
	@Test
	public void testSerialize() throws IOException {
		Assert.assertEquals(bytes.length, 4);
		Assert.assertEquals(StringUtils.byteToHexString(bytes), "000000a3");
		
//		System.out.println(bytes.length);
//		System.out.println(StringUtils.byteToHexString(bytes));
	}

	/**
	 * 创建一个没有值的IntWritable对象，并且通过调用反序列化方法将bytes的数据读入到它里面 通过调用它的get方法，获得原始的值，163
	 */
	@Test
	public void testDeserialize() throws IOException {
		IntWritable newWritable = new IntWritable();
		deserialize(newWritable, bytes);
		Assert.assertEquals(newWritable.get(), 163);
	}

	/**
	 * 将一个实现了Writable接口的对象序列化成字节流
	 * 
	 * @param writable
	 * @return
	 * @throws IOException
	 */
	public static byte[] serialize(Writable writable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		return out.toByteArray();
	}

	/**
	 * 将字节流转化为实现了Writable接口的对象
	 * 
	 * @param writable
	 * @param bytes
	 * @return
	 * @throws IOException
	 */
	public static byte[] deserialize(Writable writable, byte[] bytes)
			throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
		return bytes;
	}
	
	/**
	 * 变长格式编码方式
	 */
	@Test
	public void testVInt() throws IOException {
	byte[] data = serialize(new VIntWritable(163));
	Assert.assertEquals(StringUtils.byteToHexString(data), "8fa3");
//	System.out.println(StringUtils.byteToHexString(data));
	}
	
	/**
	 * Text的chatAt返回的是一个表示Unicode编码位置的int类型值，而不是象String那样的unicode编码的char类型。
	 * Text的find()方法，类似于String里的indexOf()方法.
	 */
    @Test
    public void testTextIndex(){
        Text t1=new Text("hadoop");
        Assert.assertEquals(t1.getLength(), 6);
        Assert.assertEquals(t1.getBytes().length, 6);
        Assert.assertEquals(t1.charAt(2),(int)'d');
        Assert.assertEquals("Out of bounds",t1.charAt(100),-1);
        
        Text t2 = new Text("hadoop");
        Assert.assertEquals("Find a substring", t2.find("do"), 2);
        Assert.assertEquals("Finds first 'o'", t2.find("p"), 5);
        Assert.assertEquals("Finds 'o' from position 4 or later", t2.find("o", 4), 4);
        Assert.assertEquals("No match", t2.find("pig"), -1);
    }
    
    
	
}