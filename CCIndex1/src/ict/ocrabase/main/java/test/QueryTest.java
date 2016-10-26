package ict.ocrabase.main.java.test;

import ict.ocrabase.main.java.client.index.IndexNotExistedException;
import ict.ocrabase.main.java.client.index.IndexResultScanner;
import ict.ocrabase.main.java.client.index.IndexTable;
import ict.ocrabase.main.java.client.index.Range;
import ict.ocrabase.main.java.regionserver.DataType;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
//secondary delay average 
//ccindex delay average

public class QueryTest {

	private static void printLine(Result r, Map<byte[], DataType> columnInfoMap) {
		StringBuilder sb = new StringBuilder();
		sb.append("row=" + Bytes.toString(r.getRow()));
		
		
		
		byte [] f_c=r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
		if(f_c!=null){
			sb.append(",f1_f1:"+Bytes.toLong(f_c));
		}
		
		f_c=r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
		if(f_c!=null){
			sb.append(",f1_f2:"+Bytes.toLong(f_c));
		}
		
		f_c=r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("c3"));
		if(f_c!=null){
			sb.append(",f2_f3:"+Bytes.toLong(f_c));
		}
		
		
		f_c=r.getValue(Bytes.toBytes("f3"), Bytes.toBytes("c4"));
		if(f_c!=null){
			sb.append(",f3_f4:"+Bytes.toLong(f_c));
		}
		
		
//		byte[] tmp = null;
//		for(Cell cell :r.listCells())
//		{
//			tmp = KeyValue.makeColumn(cell.getFamilyArray(), cell.getQualifierArray());
//			sb.append("," + Bytes.toString(tmp) + "=");
//			
//			if (columnInfoMap == null || columnInfoMap.isEmpty()) {
//				sb.append(Bytes.toString(cell.getValueArray()));
//			} else {
//				switch (columnInfoMap.get(tmp)) {
//				case DOUBLE:
//					sb.append(Bytes.toDouble(cell.getValueArray()));
//					break;
//				case LONG:
//					sb.append(Bytes.toLong(cell.getValueArray()));
//					break;
//				case INT:
//					sb.append(Bytes.toInt(cell.getValueArray()));
//					break;
//				case STRING:
//					sb.append(Bytes.toString(cell.getValueArray()));
//					break;
//				default:
//					sb.append(Bytes.toString(cell.getValueArray()));
//					break;
//				}
//			}
//		}
		
		
//		for (KeyValue kv : r.list()) {
//			tmp = KeyValue.makeColumn(kv.getFamily(), kv.getQualifier());
//			sb.append("," + Bytes.toString(tmp) + "=");
//
//			if (columnInfoMap == null || columnInfoMap.isEmpty()) {
//				sb.append(Bytes.toString(kv.getValue()));
//			} else {
//				switch (columnInfoMap.get(tmp)) {
//				case DOUBLE:
//					sb.append(Bytes.toDouble(kv.getValue()));
//					break;
//				case LONG:
//					sb.append(Bytes.toLong(kv.getValue()));
//					break;
//				case INT:
//					sb.append(Bytes.toInt(kv.getValue()));
//					break;
//				case STRING:
//					sb.append(Bytes.toString(kv.getValue()));
//					break;
//				default:
//					sb.append(Bytes.toString(kv.getValue()));
//					break;
//				}
//			}
//		}

		System.out.println(sb.toString());
	}

	public static void main(String[] args) throws IOException {
		Random rand = new Random();
		// File dataSource1w = new File("/root/Desktop/LR/ccindex_delay1w");
		// File dataSource5w = new File("/root/Desktop/LR/ccindex_delay5w");
		// File dataSource10w = new File("/root/Desktop/LR/ccindex_delay10w");
		// File dataSource20w = new File("/root/Desktop/LR/ccindex_delay20w");
		// File dataSource50w = new File("/root/Desktop/LR/ccindex_delay50w");
		// File dataSource1m = new File("/root/Desktop/LR/ccindex_delay1m");
		// File dataSource2m = new File("/root/Desktop/LR/ccindex_delay2m");
		// File dataSource3m = new File("/root/Desktop/LR/ccindex_delay3m");
		// File dataSource5m = new File("/root/Desktop/LR/ccindex_delay5m");
		// File dataSource7m = new File("/root/Desktop/LR/ccindex_delay7m");
		// File dataSource10m = new File("/root/Desktop/LR/ccindex_delay10m");
		// File dataSource20m = new File("/root/Desktop/LR/ccindex_delay20m");
		File datasource = new File("/opt/newqhl/CCIndex/txt");

		try {
			datasource.createNewFile();
			// dataSource1w.createNewFile();
			// dataSource5w.createNewFile();
			// dataSource10w.createNewFile();
			// dataSource20w.createNewFile();
			// dataSource50w.createNewFile();
			// dataSource1m.createNewFile();
			// dataSource2m.createNewFile();
			// dataSource3m.createNewFile();
			// dataSource5m.createNewFile();
			// dataSource7m.createNewFile();
			// dataSource10m.createNewFile();
			// dataSource20m.createNewFile();
			//

		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("create file failed");

			e.printStackTrace();
		}

		// FileWriter fileWriter = new FileWriter(dataSource);
		// FileWriter fileWriter1w = new FileWriter(dataSource1w);
		// FileWriter fileWriter5w = new FileWriter(dataSource5w);
		// FileWriter fileWriter10w = new FileWriter(dataSource10w);
		// FileWriter fileWriter20w = new FileWriter(dataSource20w);
		// FileWriter fileWriter50w = new FileWriter(dataSource50w);
		// FileWriter fileWriter1m = new FileWriter(dataSource1m);
		// FileWriter fileWriter2m = new FileWriter(dataSource2m);
		// FileWriter fileWriter3m = new FileWriter(dataSource3m);
		// FileWriter fileWriter5m = new FileWriter(dataSource5m);
		// FileWriter fileWriter7m = new FileWriter(dataSource7m);
		// FileWriter fileWriter10m = new FileWriter(dataSource10m);
		// FileWriter fileWriter20m = new FileWriter(dataSource20m);
		
	
		IndexTable indextable = new IndexTable("test");
		//indextable.setMaxScanThreads(1);
		
		
		indextable.setScannerCaching(1);
		indextable.setMaxScanThreads(1);
		System.out.println("max thread:"+indextable.getMaxScanThreads());
		
		long totaldelay = 0;

		int nums = 0;
		int cnt = 0;
		long temptime1 =System.currentTimeMillis();
		long temptime2 =System.currentTimeMillis();
		for (; nums < 1; nums++) {
//			if(nums%100==0)
//				System.out.println(nums + "  time: "+ (temptime2-temptime1));
//				
			temptime1 = temptime2;
			temptime2 = System.currentTimeMillis();
//			try {
//				Thread.sleep(500);
//			} catch (InterruptedException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
//			double startkey = rand.nextGaussian();
//			// System.out.println("startkey "+nums+" "+startkey);
//			double endkey = startkey + 1;
//			int startkey1 = rand.nextInt();
//			int endkey1 = startkey1 + 100000000;
			
			long startvalue=(long)200;
			long endvalue=(long)35000;
			
			
			Range[] ranges = new Range[2];
			ranges[0] = new Range(indextable.getTableName(),
					Bytes.toBytes("f3:c4"));
			ranges[0].setStartType(CompareOp.GREATER_OR_EQUAL);
			ranges[0].setStartValue(Bytes.toBytes(startvalue));
			ranges[0].setEndType(CompareOp.LESS);
			ranges[0].setEndValue(Bytes.toBytes(endvalue));
			
			ranges[1] = new Range(indextable.getTableName(),
					Bytes.toBytes("f1:c2"));
			ranges[1].setStartType(CompareOp.GREATER_OR_EQUAL);
			ranges[1].setStartValue(Bytes.toBytes(startvalue));
			ranges[1].setEndType(CompareOp.LESS);
			ranges[1].setEndValue(Bytes.toBytes(endvalue));
			// ranges[1] = new Range(indextable.getTableName(),
			// Bytes.toBytes("f2:c3"));
			// ranges[1].setStartType(CompareOp.GREATER_OR_EQUAL);
			// ranges[1].setStartValue(null);
			// ranges[1].setEndType(CompareOp.LESS);
			// ranges[1].setEndValue(Bytes.toBytes("3021"));
			byte[][] resultcolumn = new byte[2][];
			resultcolumn[0] = Bytes.toBytes("f2:c3");
			resultcolumn[1] = Bytes.toBytes("f1:c2");
			// resultcolumn[0] = Bytes.toBytes("f2:c3");

			try {
				long starttime1 = System.currentTimeMillis();
				IndexResultScanner rs = indextable.getScanner(
						new Range[][] { ranges }, resultcolumn);

				Result r;
				int interval = 100000;

				Map<byte[], DataType> columnMap = indextable.getColumnInfoMap();
				
				// System.out.println(rs.getTotalScannerNum() + "   "+rs.getTotalCount() +"  "+rs.getFinishedScannerNum());
				long whilecnt = 0;
				while ((r = rs.next()) != null) {
					whilecnt++;
					
					 printLine(r, columnMap);
					//System.out.println(whilecnt);
					long tookout = rs.getTookOutCount();
					// if (tookout == 10*1000 ){
//					if (tookout % 100000 ==0 ){//(whilecnt % 10000 == 0) {
						//System.out.print("delay ");
						long delaytmp = System.currentTimeMillis() - starttime1;
						FileWriter file_writer = new FileWriter(datasource,true);
						file_writer.write("No"+nums+"     "+tookout+"   "+Long.toString(delaytmp)+'\n');
						System.out.println("No"+nums+"    "+tookout+"   "+  Long.toString(delaytmp)+'\n');
						file_writer.close();
						totaldelay += delaytmp;
						cnt++;
						//rs.close();
						//break;
//					}
					
					
//					long tookout = rs.getTookOutCount();
					// if (tookout == 10*1000 ){
					// String str =
					// "------------results:" + rs.getTotalCount() + "," +
					// "time:" + rs.getTotalScanTime()
					// + "ms," + "totalscanner:" + rs.getTotalScannerNum() + ","
					// + "finishedscanner:"
					// + rs.getFinishedScannerNum() + "-------------\n";
					// System.out.println(str);
					// fileWriter1w.write(" "+rs.getTotalScanTime());
					// //printLine(r, columnMap);
					// }
					// if (tookout == 50*1000 ){
					// String str =
					// "------------results:" + rs.getTotalCount() + "," +
					// "time:" + rs.getTotalScanTime()
					// + "ms," + "totalscanner:" + rs.getTotalScannerNum() + ","
					// + "finishedscanner:"
					// + rs.getFinishedScannerNum() + "-------------\n";
					// System.out.println(str);
					// fileWriter5w.write(" "+rs.getTotalScanTime());
					// //printLine(r, columnMap);
					// }
					// if (tookout == 100*1000 ){
					// String str =
					// "------------results:" + rs.getTotalCount() + "," +
					// "time:" + rs.getTotalScanTime()
					// + "ms," + "totalscanner:" + rs.getTotalScannerNum() + ","
					// + "finishedscanner:"
					// + rs.getFinishedScannerNum() + "-------------\n";
					// System.out.println(str);
					// fileWriter10w.write(" "+rs.getTotalScanTime());
					// //printLine(r, columnMap);
					// }
					// if (tookout == 200*1000 ){
					// String str =
					// "------------results:" + rs.getTotalCount() + "," +
					// "time:" + rs.getTotalScanTime()
					// + "ms," + "totalscanner:" + rs.getTotalScannerNum() + ","
					// + "finishedscanner:"
					// + rs.getFinishedScannerNum() + "-------------\n";
					// System.out.println(str);
					// fileWriter20w.write(" "+rs.getTotalScanTime());
					// //printLine(r, columnMap);
					// }
					//
					// if (tookout == 500*1000 ){
					// String str =
					// "------------results:" + rs.getTotalCount() + "," +
					// "time:" + rs.getTotalScanTime()
					// + "ms," + "totalscanner:" + rs.getTotalScannerNum() + ","
					// + "finishedscanner:"
					// + rs.getFinishedScannerNum() + "-------------\n";
					// System.out.println(str);
					// fileWriter50w.write(" "+rs.getTotalScanTime());
					// //printLine(r, columnMap);
					// }
					//
					// if (tookout == 1000*1000 ){
					// String str =
					// "------------results:" + rs.getTotalCount() + "," +
					// "time:" + rs.getTotalScanTime()
					// + "ms," + "totalscanner:" + rs.getTotalScannerNum() + ","
					// + "finishedscanner:"
					// + rs.getFinishedScannerNum() + "-------------\n";
					// System.out.println(str);
					// fileWriter1m.write(" "+rs.getTotalScanTime());
					// //printLine(r, columnMap);
					// }
					//
					// if (tookout == 2000*1000 ){
					// String str =
					// "------------results:" + rs.getTotalCount() + "," +
					// "time:" + rs.getTotalScanTime()
					// + "ms," + "totalscanner:" + rs.getTotalScannerNum() + ","
					// + "finishedscanner:"
					// + rs.getFinishedScannerNum() + "-------------\n";
					// System.out.println(str);
					// fileWriter2m.write(" "+rs.getTotalScanTime());
					// //printLine(r, columnMap);
					// }
					//
					// if (tookout == 3000*1000 ){
					// String str =
					// "------------results:" + rs.getTotalCount() + "," +
					// "time:" + rs.getTotalScanTime()
					// + "ms," + "totalscanner:" + rs.getTotalScannerNum() + ","
					// + "finishedscanner:"
					// + rs.getFinishedScannerNum() + "-------------\n";
					// System.out.println(str);
					// fileWriter3m.write(" "+rs.getTotalScanTime());
					// //printLine(r, columnMap);
					// }
					//
					// if (tookout == 5000*1000 ){
					// String str =
					// "------------results:" + rs.getTotalCount() + "," +
					// "time:" + rs.getTotalScanTime()
					// + "ms," + "totalscanner:" + rs.getTotalScannerNum() + ","
					// + "finishedscanner:"
					// + rs.getFinishedScannerNum() + "-------------\n";
					// System.out.println(str);
					// fileWriter5m.write(" "+rs.getTotalScanTime());
					// //printLine(r, columnMap);
					// }
					//
					// if (tookout == 7000*1000 ){
					// String str =
					// "------------results:" + rs.getTotalCount() + "," +
					// "time:" + rs.getTotalScanTime()
					// + "ms," + "totalscanner:" + rs.getTotalScannerNum() + ","
					// + "finishedscanner:"
					// + rs.getFinishedScannerNum() + "-------------\n";
					// System.out.println(str);
					// fileWriter7m.write(" "+rs.getTotalScanTime());
					// //printLine(r, columnMap);
					// }
					//
					// if (tookout == 10000*1000 ){
					// String str =
					// "------------results:" + rs.getTotalCount() + "," +
					// "time:" + rs.getTotalScanTime()
					// + "ms," + "totalscanner:" + rs.getTotalScannerNum() + ","
					// + "finishedscanner:"
					// + rs.getFinishedScannerNum() + "-------------\n";
					// System.out.println(str);
					// fileWriter10m.write(" "+rs.getTotalScanTime());
					// //printLine(r, columnMap);
					// }
					//
					// if (tookout == 20000*1000 ){
					// String str =
					// "------------results:" + rs.getTotalCount() + "," +
					// "time:" + rs.getTotalScanTime()
					// + "ms," + "totalscanner:" + rs.getTotalScannerNum() + ","
					// + "finishedscanner:"
					// + rs.getFinishedScannerNum() + "-------------\n";
					// System.out.println(str);
					// fileWriter20m.write(" "+rs.getTotalScanTime());
					// //printLine(r, columnMap);
					// }

				}
				//end while
				//System.out.println("whilecnt   "+whilecnt);

			} catch (IndexNotExistedException e) {
				// TODO Auto-generated catch block
				System.err.println("error query");
				e.printStackTrace();
			}

		}// endfor
			// System.out.println(totaldelay+" "+ totaldelay/cnt*1.0);
			// fileWriter1w.close();
		// fileWriter5w.close();
		// fileWriter10w.close();
		// fileWriter20w.close();
		// fileWriter50w.close();
		// fileWriter1m.close();
		// fileWriter2m.close();
		// fileWriter3m.close();
		// fileWriter5m.close();
		// fileWriter7m.close();
		// fileWriter10m.close();
		// fileWriter20m.close();
		

	}

}
