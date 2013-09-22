package com.tht.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class Hello {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws TableNotFoundException 
	 */
	public static void main(String[] args) throws TableNotFoundException, IOException {
		// TODO Auto-generated method stub
		 Configuration conf = HBaseConfiguration.create();
		  conf.set("hbase.zookeeper.quorum", "master");
		  conf.set("hbase.zookeeper.property.clientPort", "2181");
		  HBaseAdmin admin = new HBaseAdmin(conf);
		  HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes("users"));
		  byte[] name = tableDescriptor.getName();
		  System.out.println("下面开始输出结果：");

		  System.out.println("表名："+ new String(name));
		  HColumnDescriptor[] columnFamilies = tableDescriptor
				  .getColumnFamilies();
		  for(HColumnDescriptor d : columnFamilies){
			  System.out.println("列族名:"+ d.getNameAsString());
			  }

	}

}
