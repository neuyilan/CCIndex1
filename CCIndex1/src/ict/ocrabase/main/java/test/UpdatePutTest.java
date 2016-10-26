package ict.ocrabase.main.java.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class UpdatePutTest {
	
	
	public static void main(String args[]) throws IOException{
		updatePut();
	}
	
	
	public static void updatePut() throws IOException{
		Configuration conf= HBaseConfiguration.create();
		String tableName="test";
		HTable table=new HTable(conf,tableName);
		Put put=new Put(Bytes.toBytes("10"));
		put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes((long)111));
		put.add(Bytes.toBytes("f1"), Bytes.toBytes("c2"), Bytes.toBytes((long)222));
		put.add(Bytes.toBytes("f2"), Bytes.toBytes("c3"), Bytes.toBytes((long)333));
		put.add(Bytes.toBytes("f3"), Bytes.toBytes("c4"), Bytes.toBytes((long)444));
		table.put(put);
		table.close();
	}
	
	
	
}
