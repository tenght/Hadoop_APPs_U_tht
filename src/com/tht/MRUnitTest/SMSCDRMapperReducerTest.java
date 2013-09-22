/**
 * 下面的示例将用MRUnit去单元测试一个SMS CDR（呼叫明细记录）分析的Map Reduce程序。
记录示例如下：
    CDRID; CDRType; Phone1; Phone2; SMS Status Code
    655209;1;796764372490213;804422938115889;6
    353415;0;356857119806206;287572231184798;4
    835699;1;252280313968413;889717902341635;0
此MapReduce程序分析这些记录，找到所有记录中CDRType为1的，并记录相对应的SMS Status Code，例如
这个Mapper输出为：
6, 1
0, 1
 Reducer任务将此输出作为输入，并输出CDR记录中包含的status code具体次数
 * @author hadoop
 *
 */

package com.tht.MRUnitTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class SMSCDRMapperReducerTest {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		SMSCDRMapper mapper = new SMSCDRMapper();
		SMSCDRReducer reducer = new SMSCDRReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		;
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	 @Test
	 public void testMapper() throws IOException {
	 mapDriver.withInput(new LongWritable(), new Text(
	 "655209;1;796764372490213;804422938115889;6"));
	 mapDriver.withOutput(new Text("6"), new IntWritable(1));
	 mapDriver.runTest();
	 }

//	@Test
//	public void testMapper() throws IOException {
//		mapDriver.withInput(new LongWritable(), new Text(
//				"655209;0;796764372490213;804422938115889;6"));
//		// mapDriver.withOutput(new Text("6"), new IntWritable(1));
//		mapDriver.runTest();
//		assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
//				.findCounter(CDRCounter.NonSMSCDR).getValue());
//	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("6"), values);
		reduceDriver.withOutput(new Text("6"), new IntWritable(2));
		reduceDriver.runTest();
	}
}