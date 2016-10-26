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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class CopyOfOutLoadTest implements Runnable{
	static IndexTable indextable;
	static File datasource;
	static Range[] ranges;
	static byte[][] resultcolumn;
	static long starttime1;
	static double result;
	public static void main(String[] args) throws IOException {
		indextable = new IndexTable("test");
		
		indextable.setScannerCaching(100000);
		int threadnum = 5;
		
		
		
		indextable.setMaxScanThreads(50);
		

			long startvalue=(long)0;
			long endvalue=(long)4000000;
			
			ranges = new Range[2];
			ranges[0] = new Range(indextable.getTableName(),
					Bytes.toBytes("f3:c4"));
			ranges[0].setStartType(CompareOp.GREATER_OR_EQUAL);
			
			
			ranges[0].setStartValue(Bytes.toBytes(startvalue));
			ranges[0].setEndType(CompareOp.LESS);
			ranges[0].setEndValue(Bytes.toBytes("endvalue"));
			
			ranges[1] = new Range(indextable.getTableName(),
					Bytes.toBytes("f1:c2"));
			ranges[1].setStartType(CompareOp.GREATER_OR_EQUAL);
			ranges[1].setStartValue(Bytes.toBytes(startvalue));
			ranges[1].setEndType(CompareOp.LESS);
			ranges[1].setEndValue(Bytes.toBytes(endvalue));
			
			
			resultcolumn = new byte[2][];
			resultcolumn[0] = Bytes.toBytes("f3:c4");
			resultcolumn[1] = Bytes.toBytes("f1:c2");
			starttime1 = System.currentTimeMillis();
			
			for(int i=0;i<threadnum;i++){
				CopyOfOutLoadTest test1 = new CopyOfOutLoadTest();

				Thread demo1= new Thread(test1);
				demo1.start();
			}
			
			
			
			
		}


	@Override
	public void run() {
		System.out.println("thread start!");
		try {
			IndexResultScanner rs = indextable.getScanner(
					new Range[][] { ranges }, resultcolumn);
			
			
			for(int i=0;i<(new Range[][] { ranges }).length;i++){
				System.out.println("%%%%%%%%%%%%%%%%%%>"+(new Range[][] { ranges })[i].toString());
				for(int j=0;j<(new Range[][] { ranges })[i].length;j++){
					System.out.println("-------------->"+(new Range[][] { ranges })[i][j]);
				}
			}
			
			Result r;
			while ((r = rs.next()) != null) {
				StringBuilder sb=new StringBuilder();
				
				String row=Bytes.toString(r.getRow());
				sb.append("row="+row);
				    
				byte[] f_c=r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
				if(f_c!=null){
					sb.append(",f1:c2="+Bytes.toLong(f_c));
				}
				
				f_c=r.getValue(Bytes.toBytes("f3"), Bytes.toBytes("c4"));
				if(f_c!=null){
					sb.append(",f3:c4= "+Bytes.toLong(f_c));
				}
				System.out.println(sb);
			}
			rs.close();

		} catch (IndexNotExistedException e) {
			System.err.println("error query");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
		

	

}
