package ict.ocrabase.main.java.test;

import ict.ocrabase.main.java.client.index.IndexExistedException;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanIndexTableTest {
	private static Configuration conf;
	private static final byte[] table_test = Bytes.toBytes("test_table_with_index");
	private static final byte[] table_test_f_c1 = Bytes.toBytes("test_table_with_index-f_c1");
	private static final byte[] table_test_f_c4 = Bytes.toBytes("test_table_with_index-f_c4");
	private static long startTime=0;
	private static long stopTime=0;
	static int count=0;
	
	
	public static void main(String[] args) throws IOException,
			IndexExistedException {

//		 scanTest();
//		scanTest__f_c1();
		scanTest__f_c4();
	}

	public static void scanTest() throws IOException {
		conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, table_test);
		startTime = System.currentTimeMillis();
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		Result res=scanner.next();
		startTime=System.currentTimeMillis();
		while(res!=null){
			println_test(res);
			res=scanner.next();
			count++;
		}
		stopTime = System.currentTimeMillis();

		System.out.println("total times cost:" + (stopTime - startTime)/1000 + " s");
		System.out.println("total row number"+count);

		table.close();
	}
	
	
	static void println_test(Result result) {
	    StringBuilder sb = new StringBuilder();
	    sb.append("row=" + Bytes.toString(result.getRow()));

	    List<KeyValue> kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c1"));
	    if (kv.size() != 0) {
	      sb.append(", f:c1=" + Bytes.toInt(kv.get(0).getValue()));
	    }

	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c2"));
	    if (kv.size() != 0) {
	      sb.append(", f:c2=" + Bytes.toString(kv.get(0).getValue()));
	    }

	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c3"));
	    if (kv.size() != 0) {
	      sb.append(", f:c3=" + Bytes.toDouble(kv.get(0).getValue()));
	    }

	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c4"));
	    if (kv.size() != 0) {
	      sb.append(", f:c4=" + Bytes.toString(kv.get(0).getValue()));
	    }
	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c5"));
	    if (kv.size() != 0) {
	      sb.append(", f:c5=" + Bytes.toString(kv.get(0).getValue()));
	    }

	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c6"));
	    if (kv.size() != 0) {
	      sb.append(", f:c6=" + Bytes.toString(kv.get(0).getValue()));
	    }
	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c7"));
	    if (kv.size() != 0) {
	      sb.append(", f:c7=" + Bytes.toInt(kv.get(0).getValue()));
	    }
	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c8"));
	    if (kv.size() != 0) {
	      sb.append(", f:c8=" + Bytes.toString(kv.get(0).getValue()));
	    }
	    System.out.println(sb.toString());
	  }

	
	public static void scanTest__f_c1() throws IOException {
		conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, table_test_f_c1);
		startTime = System.currentTimeMillis();
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		Result res=scanner.next();
		startTime=System.currentTimeMillis();
		while(res!=null){
			println_test_f_c1(res);
			res=scanner.next();
			count++;
		}
		stopTime = System.currentTimeMillis();

		System.out.println("total times cost:" + (stopTime - startTime)/1000 + " s");
		System.out.println("total row number"+count);

		table.close();
	}

	
	static void println_test_f_c1(Result result) {
	    StringBuilder sb = new StringBuilder();
	    sb.append("row=" + Bytes.toInt(result.getRow()));

	    List<KeyValue> kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c2"));
	    if (kv.size() != 0) {
	      sb.append(", f:c2=" + Bytes.toString(kv.get(0).getValue()));
	    }

	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c3"));
	    if (kv.size() != 0) {
	      sb.append(", f:c3=" + Bytes.toDouble(kv.get(0).getValue()));
	    }

	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c4"));
	    if (kv.size() != 0) {
	      sb.append(", f:c4=" + Bytes.toString(kv.get(0).getValue()));
	    }
	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c5"));
	    if (kv.size() != 0) {
	      sb.append(", f:c5=" + Bytes.toString(kv.get(0).getValue()));
	    }

	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c6"));
	    if (kv.size() != 0) {
	      sb.append(", f:c6=" + Bytes.toString(kv.get(0).getValue()));
	    }
	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c7"));
	    if (kv.size() != 0) {
	      sb.append(", f:c7=" + Bytes.toInt(kv.get(0).getValue()));
	    }
	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c8"));
	    if (kv.size() != 0) {
	      sb.append(", f:c8=" + Bytes.toString(kv.get(0).getValue()));
	    }
	    System.out.println(sb.toString());
	  }
	
	public static void scanTest__f_c4() throws IOException {
		conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, table_test_f_c4);
		startTime = System.currentTimeMillis();
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		Result res=scanner.next();
		startTime=System.currentTimeMillis();
		while(res!=null){
			println_test_f_c4(res);
			res=scanner.next();
			count++;
		}
		stopTime = System.currentTimeMillis();

		System.out.println("total times cost:" + (stopTime - startTime)/1000 + " s");
		System.out.println("total row number"+count);

		table.close();
	}

	
	static void println_test_f_c4(Result result) {
	    StringBuilder sb = new StringBuilder();
	    sb.append("row=" + Bytes.toInt(result.getRow()));

	    List<KeyValue> kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c1"));
	    if (kv.size() != 0) {
	      sb.append(", f:c1=" + Bytes.toInt(kv.get(0).getValue()));
	    }
	    
	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c2"));
	    if (kv.size() != 0) {
	      sb.append(", f:c2=" + Bytes.toString(kv.get(0).getValue()));
	    }

	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c3"));
	    if (kv.size() != 0) {
	      sb.append(", f:c3=" + Bytes.toDouble(kv.get(0).getValue()));
	    }

//	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c4"));
//	    if (kv.size() != 0) {
//	      sb.append(", f:c4=" + Bytes.toString(kv.get(0).getValue()));
//	    }
	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c5"));
	    if (kv.size() != 0) {
	      sb.append(", f:c5=" + Bytes.toString(kv.get(0).getValue()));
	    }

	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c6"));
	    if (kv.size() != 0) {
	      sb.append(", f:c6=" + Bytes.toString(kv.get(0).getValue()));
	    }
	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c7"));
	    if (kv.size() != 0) {
	      sb.append(", f:c7=" + Bytes.toInt(kv.get(0).getValue()));
	    }
	    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c8"));
	    if (kv.size() != 0) {
	      sb.append(", f:c8=" + Bytes.toString(kv.get(0).getValue()));
	    }
	    System.out.println(sb.toString());
	  }
	
	
	

	
}
