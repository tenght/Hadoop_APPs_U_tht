package com.tht.hadoopIO;

//cc TextIterator Iterating over the characters in a Text object
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;

//vv TextIterator
/**
 * 迭代：将Text对象变成java.io.ByteBuffer，然后对缓冲的Text反复调用bytesToCodePoint（）静态方法。
 * 这个方法提取下一个代码点作为int然后更新缓冲中的位置。当bytesToCodePoint（）返回-1时，检测到字符串结束。
 * 下面程序遍历Text对象中的字符：
 * @author hadoop
 *
 */
public class TextIterator {

	public static void main(String[] args) {
		Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
//		t.set("\u0041\u00DF\u6771\uD801");
		ByteBuffer buf = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());
		int cp;
		while (buf.hasRemaining() && (cp = Text.bytesToCodePoint(buf)) != -1) {
			System.out.println(Integer.toHexString(cp));
		}
	}
}
// ^^ TextIterator
