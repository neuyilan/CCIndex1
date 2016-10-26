package ict.ocrabase.main.java.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteIndexTest {
	public DeleteIndexTest() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration conf = HBaseConfiguration.create();
		
		
		String tableName="test";
		 HTable table = new HTable(conf, tableName);
		 Delete delete = new Delete(Bytes.toBytes("21"));
//		 delete.addColumn(Bytes.toBytes("f"), Bytes.toBytes("f3"));
		 delete.deleteColumns(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
//		 delete.addColumns(Bytes.toBytes("f"), Bytes.toBytes("f3"),Integer.MAX_VALUE);
		 table.delete(delete);
		 table.close();
	}
	
	public static void main(String args[]) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		new DeleteIndexTest();
	}
}
