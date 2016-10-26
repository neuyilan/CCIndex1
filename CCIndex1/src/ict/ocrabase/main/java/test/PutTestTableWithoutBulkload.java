package ict.ocrabase.main.java.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutTestTableWithoutBulkload {
	static String filePath = "/opt/newqhl/CCIndex/test-data/CCIndex-bulkload-index.txt";
	static boolean wal = false;
//	static int writeNum = 500;

	ArrayList<Put> queue = new ArrayList<Put>();
	String tableName = "test_table_without_bulkload";
	Configuration conf = HBaseConfiguration.create();

	Writer writer = null;
	long startTime = 0;
	long stopTime=0;
	long writeCount = 0;

	public PutTestTableWithoutBulkload() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);

		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		HColumnDescriptor family = new HColumnDescriptor("f");
		family.setMaxVersions(10);
		family.setMinVersions(3);

		tableDesc.addFamily(family);
		admin.createTable(tableDesc, Bytes.toBytes("1"), Bytes.toBytes("9"), 10);
		admin.close();
	}

	public void start() throws IOException {
		writer = new Writer();
		writer.setName("Writer");
		writer.start();
	}

	public void stop() {
		try {
			writer.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	class Writer extends Thread {

		private byte[] reverse(byte[] b) {
			for (int i = 0, j = b.length - 1; i < j; i++, j--) {
				byte tmp = b[i];
				b[i] = b[j];
				b[j] = tmp;
			}
			return b;
		}

		public void loadData() {
			try {
				BufferedReader reader = new BufferedReader(new FileReader(
						new File(filePath)));
				String line = null, col[] = null;
				HTable table = new HTable(conf, tableName);
				
				startTime = System.currentTimeMillis();
				System.out.println("time begin------>:"+startTime);
				
				while ((line = reader.readLine()) != null) {
					col = line.split(";");
					Put put = new Put(reverse(Bytes.toBytes(col[0])));
					put.add(Bytes.toBytes("f"), Bytes.toBytes("c1"),
							Bytes.toBytes(Integer.valueOf(col[1]))); // int
					put.add(Bytes.toBytes("f"), Bytes.toBytes("c2"),
							Bytes.toBytes(col[2])); // string
					put.add(Bytes.toBytes("f"), Bytes.toBytes("c3"),
							Bytes.toBytes(Double.valueOf(col[3]))); // double
					put.add(Bytes.toBytes("f"), Bytes.toBytes("c4"),
							Bytes.toBytes(col[4])); // string
					put.add(Bytes.toBytes("f"), Bytes.toBytes("c5"),
							Bytes.toBytes(col[5])); // string
					put.add(Bytes.toBytes("f"), Bytes.toBytes("c6"),
							Bytes.toBytes(col[6])); // string
					put.add(Bytes.toBytes("f"), Bytes.toBytes("c7"),
							Bytes.toBytes(Integer.valueOf(col[7]))); // int
					put.add(Bytes.toBytes("f"), Bytes.toBytes("c8"),
							Bytes.toBytes(col[8])); // string
					if (!wal) {
						put.setDurability(Durability.SKIP_WAL);
					}
//					queue.add(put);
//					if (queue.size() > writeNum) {
//						break;
//					}
					writeCount++;
					table.put(put);
				}
				
				stopTime = System.currentTimeMillis();
				System.out.println("time stop------>:"+stopTime);
				System.out.println("total estimatedTime: "
						+ (stopTime-startTime)/1000 + " s");
				System.out.println("every row estimated time:"
						+ (stopTime-startTime)/writeCount/1000+" s");

				reader.close();
				table.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		

		public void run() {
			// 1s=1,000 millisecond
			loadData();
//			try {
//				long stopTime = 0;
//				HTable table = new HTable(conf, tableName);
//				startTime = System.currentTimeMillis();
//				System.out.println("time begin------>:"+startTime);
//				for (Put put : queue) {
//					table.put(put);
//					writeCount++;
//				}
//				stopTime = System.currentTimeMillis();
//				System.out.println("time stop------>:"+stopTime);
//				table.close();
//				System.out.println("total estimatedTime: "
//						+ (stopTime-startTime)/1000 + " s");
//
//				System.out.println("every row estimated time:"
//						+ (stopTime-startTime)/writeCount/1000+" s");
//
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
		}
	}

	public static void main(String[] args) throws IOException {
		try {
			filePath = args[0];
//			writeNum = Integer.valueOf(args[1]);
			wal = Boolean.valueOf(args[2]);
		} catch (Exception e) {
			System.out.println("filePath  writeNum  wal index");
		}

		System.out.println("----------------" + filePath);
//		System.out.println("----------------" + writeNum);
		System.out.println("----------------" + wal);

		PutTestTableWithoutBulkload test = new PutTestTableWithoutBulkload();
		test.start();
		// while (test.writer.isAlive()) {
		// // try {
		// // Thread.sleep(5000);
		// // } catch (InterruptedException e) {
		// // e.printStackTrace();
		// // }
		// // test.report();
		// }
		//
		// test.report();
	}

}
