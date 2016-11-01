package ict.ocrabase.main.java.test;

import ict.ocrabase.main.java.client.index.IndexNotExistedException;
import ict.ocrabase.main.java.client.index.IndexResultScanner;
import ict.ocrabase.main.java.client.index.IndexTable;
import ict.ocrabase.main.java.client.index.Range;
import ict.ocrabase.main.java.regionserver.DataType;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * select orderkey, orderdate, shippriority from orders where  ? <orderdate and  orderdate < ?
 * @author houliang
 */
public class OrderQ5 {
	
	
	public static String println_test(Result result) {
		StringBuilder sb = new StringBuilder();
		sb.append("row=" + Bytes.toString(result.getRow()));

		List<KeyValue> kv = result.getColumn(Bytes.toBytes("f"),
				Bytes.toBytes("c1"));
		if (kv.size() != 0) {
			sb.append(", f:c1=" + Bytes.toString(kv.get(0).getValue()));
		}

		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c2"));
		if (kv.size() != 0) {
			sb.append(", f:c2=" + Bytes.toString(kv.get(0).getValue()));
		}

		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c3"));
		if (kv.size() != 0) {
			sb.append(", f:c3=" + Bytes.toString(kv.get(0).getValue()));
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
			sb.append(", f:c7=" + Bytes.toString(kv.get(0).getValue()));
		}
		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c8"));
		if (kv.size() != 0) {
			sb.append(", f:c8=" + Bytes.toString(kv.get(0).getValue()));
		}
		System.out.println(sb.toString());
		return sb.toString();
		
	}

	public static void queryTest(String startOrderDate, String endOrderDate,
			String tableName, int scanCache, int threads)
			throws IOException {
		FileWriter fileWriter;
		
		IndexTable indextable = new IndexTable(tableName);
		indextable.setScannerCaching(scanCache);
		indextable.setMaxScanThreads(threads);
		// System.out.println("max thread:" + indextable.getMaxScanThreads());

		Range[] ranges = new Range[1];
		ranges[0] = new Range(indextable.getTableName(), Bytes.toBytes("f:c4"));
		ranges[0].setStartType(CompareOp.GREATER);
		ranges[0].setStartValue(Bytes.toBytes(startOrderDate));
		ranges[0].setEndType(CompareOp.LESS);
		ranges[0].setEndValue(Bytes.toBytes(endOrderDate));

		byte[][] resultcolumn = new byte[1][];
		resultcolumn[0] = Bytes.toBytes("f:c5");
		try {
			IndexResultScanner rs = indextable.getScanner(
					new Range[][] { ranges }, resultcolumn);

			Result r;

			Map<byte[], DataType> columnMap = indextable.getColumnInfoMap();

			// System.out.println(rs.getTotalScannerNum() +
			// "   "+rs.getTotalCount() +"  "+rs.getFinishedScannerNum());
			long count = 0;
			while ((r = rs.next()) != null) {
				count++;
//				println_test(r);
//				fileWriter.write(println_test(r)+"\n");
//				fileWriter.flush();
			}
//			fileWriter.close();
			System.out.println("count:"+count);
		} catch (IndexNotExistedException e) {
			System.err.println("error query");
			e.printStackTrace();
		}
		
	}
	
//	public static void write2file(String str,String saveFile){
//		try {
//			FileWriter fileWriter = new FileWriter(datasource, true);
//			fileWriter.write(str);
//			fileWriter.flush();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}

	public static void main(String[] args) throws IOException {
		if (args.length != 5) {
			System.out.println("wrong parameter");
			return;
		}
		String startOrderDate = args[0];
		String endOrderDate = args[1];
		String tableName = args[2];
		int scanCache = Integer.parseInt(args[3]);
		int threads = Integer.parseInt(args[4]);
		
		long startTime = System.currentTimeMillis();
		queryTest(startOrderDate, endOrderDate, tableName, scanCache, threads);
		long endTime = System.currentTimeMillis();
		System.out.println("endtime - starttime = " + (endTime - startTime)
				+ " ms");

	}
	
//	1994-03-15 1994-04-15 /home/qhl/ccindex/test-result/orders_out/q5  test_table 1000 10
	
	
}
