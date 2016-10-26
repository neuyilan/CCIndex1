package ict.ocrabase.main.java.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class MultipleVersionScanTest {
	

	public static void main(String[] args) throws IOException {
//		updatePut();
		readMulVersion();
	}
	
	
	
	
	
	private static void updatePut() throws IOException {
		String tableName = "test";
		Configuration conf = HBaseConfiguration.create();
		HTable table = null;
		table = new HTable(conf, tableName);
		Put put = new Put(Bytes.toBytes("10"));
		put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"),  Bytes.toBytes((long)1111));
		put.add(Bytes.toBytes("f1"), Bytes.toBytes("c2"),  Bytes.toBytes((long)2222));
		table.put(put);

		put = new Put(Bytes.toBytes("11"));
		put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"),  Bytes.toBytes((long)11111));
		put.add(Bytes.toBytes("f1"), Bytes.toBytes("c2"),  Bytes.toBytes((long)22222));
		table.put(put);

		Scan scan = new Scan();
		Result result = null;
		ResultScanner rs = table.getScanner(scan);

		while ((result = rs.next()) != null) {
//			System.out.println(result);
		}
		rs.close();

		table.close();
		
	}





	public static void readMulVersion() throws IOException {
		String tableName = "test";
		Configuration conf = HBaseConfiguration.create();
		Get get = new Get(Bytes.toBytes("10"));
		get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
		get.setMaxVersions();

		HTable table = new HTable(conf, tableName);
		Result result = table.get(get);
		
		List<Cell> list=result.listCells();
		StringBuilder sb=new StringBuilder();
		for(Cell cell:list){
			String rowkey=new String(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength(),"UTF-8");
			sb.append("row="+rowkey);
			
			String family=new String(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength(),"UTF-8");
			sb.append(",family="+family);
			
			String qualifier=new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength(),"UTF-8");
			sb.append(",qualifier="+qualifier);
			
//			String value=new String(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength(),"UTF-8");
			
			Long value = Bytes.toLong(cell.getValue());
			sb.append(",value="+value);
			
			long timestamp=cell.getTimestamp();
			sb.append(",timestamp="+timestamp);
			sb.append("\n");
		}

		System.out.println(sb);
		
//		println(result);

		table.close();

	}


	private static void println(Result res) {
		if (res == null) {
			System.out.println("null");
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
	
	
}
