package ict.ocrabase.main.java.test;

import ict.ocrabase.main.java.client.index.IndexAdmin;
import ict.ocrabase.main.java.client.index.IndexExistedException;
import ict.ocrabase.main.java.client.index.IndexSpecification;
import ict.ocrabase.main.java.client.index.IndexTableDescriptor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanTest {
	private static final byte[] tableName = Bytes.toBytes("t100M");
	private static final byte[] newTableName = Bytes.toBytes("New");
	private static Configuration conf;
	private static IndexAdmin indexadmin;
	private static IndexTableDescriptor indexDesc;
	private static HBaseAdmin admin;
	private static IndexSpecification[] index;
	private static HTableDescriptor desc;
	private static HTableDescriptor newdesc;
	private static IndexTableDescriptor newIndexDesc;
	public static void main(String[] args) throws IOException,
			IndexExistedException {

		 scanTest();
//		 scanTest_f1_c1();
		 scanTest_f1_c2();
//		scanTest_f2_c3();
//		scanTest_f3_c4();

	}

	public static void scanTest() throws IOException {
		String tableName = "test";
		conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tableName);
		long starttime = System.currentTimeMillis();
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		int i = 0;

		for (;; i++) {
			Result res = scanner.next();
			if (res == null) {
				System.out.println("null");
				break;
			}

			StringBuilder sb = new StringBuilder();
			sb.append("row=" + Bytes.toString(res.getRow()));

			List<KeyValue> kv = res.getColumn(Bytes.toBytes("f1"),
					Bytes.toBytes("c1"));
			if (kv.size() != 0) {
				sb.append(", f1:c1=" + Bytes.toLong(kv.get(0).getValue()));
			}

			kv = res.getColumn(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
			if (kv.size() != 0) {
				System.out.println(kv.get(0).getValue()+"^^^^^^^^^^^^^^^");
				sb.append(", f1:c2=" + Bytes.toLong(kv.get(0).getValue()));
			}

			kv = res.getColumn(Bytes.toBytes("f2"), Bytes.toBytes("c3"));
			if (kv.size() != 0) {
				sb.append(", f2:c3=" + Bytes.toLong(kv.get(0).getValue()));
			}

			kv = res.getColumn(Bytes.toBytes("f3"), Bytes.toBytes("c4"));
			if (kv.size() != 0) {
				sb.append(", f3:c4=" + Bytes.toLong(kv.get(0).getValue()));
			}

			System.out.println(sb.toString());

		}
		long stoptime = System.currentTimeMillis();

		System.out
				.println("total times cost:" + (stoptime - starttime) + " ms");

		table.close();
	}

	
	/**
	 * 'test-f1_c1', {TABLE_ATTRIBUTES => {METADATA => {'INDEX_TYPE' => 'SECONDARYINDEX'}}, {NAME => 'f1'}
	 * @throws IOException
	 */
	public static void scanTest_f1_c1() throws IOException {
		String tableName = "test-f1_c1";
		conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tableName);
		long starttime = System.currentTimeMillis();
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		int i = 0;

		for (;; i++) {
			Result res = scanner.next();
			if (res == null) {
				System.out.println("null");
				break;
			}

			StringBuilder sb = new StringBuilder();
			sb.append("row=" + Bytes.toLong(res.getRow()));

			byte[] f_c = res.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
			if (f_c != null) {
				sb.append(", f1:c1=" + Bytes.toLong(f_c));
			}

			System.out.println(sb.toString());

		}
		long stoptime = System.currentTimeMillis();

		System.out
				.println("total times cost:" + (stoptime - starttime) + " ms");

		table.close();
	}

	/**
	 * 'test-f1_c2', {TABLE_ATTRIBUTES => {METADATA => {'INDEX_TYPE' => 'CCINDEX'}}, {NAME => 'f1'}, {NAME => 'f2'}, {NAME => 'f3'}, {NAME => 'f4'}
	 * @throws IOException
	 */
	public static void scanTest_f1_c2() throws IOException {
		String tableName = "test-f1_c2";
		conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tableName);
		long starttime = System.currentTimeMillis();
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		int i = 0;

		for (;; i++) {
			Result res = scanner.next();
			if (res == null) {
				System.out.println("null");
				break;
			}

			StringBuilder sb = new StringBuilder();
			System.out.println(res.getRow()+"      ********");
			sb.append("row=" + Bytes.toLong(res.getRow()));
			
			
			List<KeyValue> kv = res.getColumn(Bytes.toBytes("f1"),
					Bytes.toBytes("c1"));
			if (kv.size() != 0) {
				sb.append(", f1:c1=" + Bytes.toLong(kv.get(0).getValue()));
			}

			kv = res.getColumn(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
			if (kv.size() != 0) {
				sb.append(", f1:c2=" + Bytes.toLong(kv.get(0).getValue()));
			}

			kv = res.getColumn(Bytes.toBytes("f2"), Bytes.toBytes("c3"));
			if (kv.size() != 0) {
				sb.append(", f2:c3=" + Bytes.toLong(kv.get(0).getValue()));
			}

			kv = res.getColumn(Bytes.toBytes("f3"), Bytes.toBytes("c4"));
			if (kv.size() != 0) {
				sb.append(", f3:c4=" + Bytes.toLong(kv.get(0).getValue()));
			}

			System.out.println(sb.toString());
			
			
			

//			byte[] f_c = res.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
//			if (f_c != null) {
//				sb.append(", f1:c1=" + Bytes.toLong(f_c));
//			}
//
//			f_c = res.getValue(Bytes.toBytes("f2"), Bytes.toBytes("c3"));
//			if (f_c != null) {
//				sb.append(", f2:c3=" + Bytes.toLong(f_c));
//			}
//
//			f_c = res.getValue(Bytes.toBytes("f3"), Bytes.toBytes("c4"));
//			if (f_c != null) {
//				sb.append(", f3:c4=" + Bytes.toLong(f_c));
//			}
//
//			System.out.println(sb.toString());

		}
		long stoptime = System.currentTimeMillis();

		System.out
				.println("total times cost:" + (stoptime - starttime) + " ms");

		table.close();
	}

	
	/**
	 * 'test-f2_c3', {TABLE_ATTRIBUTES => {METADATA => {'INDEX_TYPE' => 'IMPSECONDARYINDEX'}}, {NAME => 'f1'}
	 * @throws IOException
	 */
	public static void scanTest_f2_c3() throws IOException {
		String tableName = "test-f2_c3";
		conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tableName);
		long starttime = System.currentTimeMillis();
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		int i = 0;

		for (;; i++) {
			Result res = scanner.next();
			if (res == null) {
				System.out.println("null");
				break;
			}

			StringBuilder sb = new StringBuilder();
			sb.append("row=" + Bytes.toLong(res.getRow()));

			byte[] f_c = res.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
			if (f_c != null) {
				sb.append(", f1:c1=" + Bytes.toLong(f_c));
			}

			f_c = res.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
			if (f_c != null) {
				sb.append(", f1:c2=" + Bytes.toLong(f_c));
			}
			System.out.println(sb.toString());

		}
		long stoptime = System.currentTimeMillis();

		System.out
				.println("total times cost:" + (stoptime - starttime) + " ms");

		table.close();
	}

	
	/**
	 * 'test-f3_c4', {TABLE_ATTRIBUTES => {METADATA => {'INDEX_TYPE' => 'CCINDEX'}}, {NAME => 'f1'}, {NAME => 'f2'}, {NAME => 'f3'}, {NAME => 'f4'}
	 * @throws IOException
	 */
	public static void scanTest_f3_c4() throws IOException {
		String tableName = "test-f3_c4";
		conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tableName);
		long starttime = System.currentTimeMillis();
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		int i = 0;

		for (;; i++) {
			Result res = scanner.next();
			if (res == null) {
				System.out.println("null");
				break;
			}

			StringBuilder sb = new StringBuilder();
			sb.append("row=" + Bytes.toLong(res.getRow()));

			byte[] f_c = res.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
			if (f_c != null) {
				sb.append(", f1:c1=" + Bytes.toLong(f_c));
			}

			f_c = res.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
			if (f_c != null) {
				sb.append(", f1:c2=" + Bytes.toLong(f_c));
			}

			f_c = res.getValue(Bytes.toBytes("f2"), Bytes.toBytes("c3"));
			if (f_c != null) {
				sb.append(", f2:c3=" + Bytes.toLong(f_c));
			}

			System.out.println(sb.toString());

		}
		long stoptime = System.currentTimeMillis();

		System.out
				.println("total times cost:" + (stoptime - starttime) + " ms");
	}
}
