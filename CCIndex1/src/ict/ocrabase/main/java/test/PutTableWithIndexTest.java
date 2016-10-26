package ict.ocrabase.main.java.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutTableWithIndexTest {
	static String filePath = "/opt/newqhl/CCIndex/test-data/xaa-2000";
	static boolean wal = false;

	 private static final byte[] tableName = Bytes.toBytes("test_table_with_index");
	Configuration conf = HBaseConfiguration.create();

	Writer writer = null;
	long startTime = 0;
	long stopTime=0;
	long writeCount = 0;

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
				 String attr=conf.get("hbase.coprocessor.region.classes");
				    System.out.println("-------------->"+attr.toString());
				BufferedReader reader = new BufferedReader(new FileReader(
						new File(filePath)));
				String line = null, col[] = null;

				// key ORDERKEY Int
				// c1 CUSTKEY Int
				// c2 ORDERSTATUS String
				// c3 TOTALPRICE Double index
				// c4 ORDERDATE String index
				// c5 ORDERPRIORITY String index
				// c6 CLERK String
				// c7 SHIPPRIORITY Int
				// c8 COMMENT String

				HTable table = new HTable(conf, tableName);

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

					writeCount++;
					table.put(put);
				}
				table.close();
				reader.close();

				System.out.println("writeCount:" + writeCount);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void run() {
			startTime = System.currentTimeMillis();
			loadData();
			stopTime=System.currentTimeMillis();
			System.out.println("total estimatedTime: "
					+ (stopTime-startTime)/1000 + " s");
			System.out.println("put the  row to table :"
					+ (stopTime - startTime) / writeCount / 1000);
		}
	}

	public static void main(String[] args) throws IOException {
		try {
			filePath = args[0];
			wal = Boolean.valueOf(args[1]);
		} catch (Exception e) {
			System.out.println("filePath  writeNum  wal index");
		}

		System.out.println("----------------" + filePath);
		System.out.println("----------------" + wal);
		PutTableWithIndexTest test = new PutTableWithIndexTest();
		test.start();
	}

}
