package ict.ocrabase.main.java.client.bulkload.index;

import java.io.IOException;
import java.lang.reflect.Constructor;

import ict.ocrabase.main.java.client.bulkload.BulkLoadUtil;
import ict.ocrabase.main.java.client.bulkload.ImportConstants;
import ict.ocrabase.main.java.client.bulkload.TableInfo;
import ict.ocrabase.main.java.client.index.IndexKeyGenerator;
import ict.ocrabase.main.java.client.index.SimpleIndexKeyGenerator;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NoKeyTextIndexMapper extends
	Mapper<LongWritable, Text, ImmutableBytesWritable, Text>{
	
	private TableInfo table;
	private int[] indexPos;
	private IndexKeyGenerator ikg;
	
	private int lineCounter;
	private int taskID;
	private String firstZero;
	private String secondZero;
	
	
	final static int[] sizeTable = { 9, 99, 999, 9999, 99999, 999999, 9999999,
		99999999, 999999999, Integer.MAX_VALUE };
	private int sizeOfInt(int x) {
		for (int i = 0;; i++)
			if (x <= sizeTable[i])
				return i + 1;
	}
	
	private String makeZero(int len) {
		String zero = "";
		for (int i = 0; i < len; i++) {
			zero = zero + "0";
		}
		return zero;
	}
	
	private byte[] getRowkey() {
		StringBuffer strBuf = new StringBuffer();
		strBuf.append(firstZero.substring(sizeOfInt(taskID)));
		strBuf.append(taskID);
		strBuf.append(secondZero.substring(sizeOfInt(lineCounter)));
		strBuf.append(lineCounter);
		return Bytes.toBytes(strBuf.toString());
	}
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		table = new TableInfo(context.getConfiguration().get(
				ImportConstants.BULKLOAD_DATA_FORMAT));
		indexPos = table.getIndexPos();
		try {
			Constructor<?> cons = context.getConfiguration().getClass("bulkload.indexKeyGenerator", SimpleIndexKeyGenerator.class).getConstructor();
			ikg = (IndexKeyGenerator)cons.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		lineCounter = 0;
		int firstLen = context.getConfiguration().getInt(
				"incremental.rowkey.first", 15);
		int secondLen = context.getConfiguration().getInt(
				"incremental.rowkey.second", 15);
		firstZero = makeZero(firstLen);
		secondZero = makeZero(secondLen);
		taskID = context.getTaskAttemptID().getTaskID().getId()
				+ context.getConfiguration().getInt("incremental.max.id", 0);
	}
	
	protected void map(LongWritable key1, Text value, Mapper<LongWritable,Text,ImmutableBytesWritable,Text>.Context context) throws java.io.IOException ,InterruptedException {
		lineCounter++;
		
		byte[] key = getRowkey();
		byte[] rowkey = new byte[1 + key.length];
		Bytes.putByte(rowkey, 0, (byte) 0);
		Bytes.putBytes(rowkey, 1, key, 0, key.length);
		context.write(new ImmutableBytesWritable(rowkey), value);
		
		byte[] line = value.getBytes();
		int lineLen = value.getLength();
		Integer[] split = BulkLoadUtil.dataSplit(table.getSeparator(), line,
				lineLen);
		int count = 1;
		byte[] valueBytes;
		for (int p : indexPos) {
			if(split[p]+1 == split[p+1]){
				count++;
				continue;
			}
			TableInfo.ColumnInfo col = table.getColumnInfo(p);
			valueBytes = BulkLoadUtil.convertToValueBytes(col.getDataType(), line, split[p]+1, split[p+1]-split[p]-1);
			byte[] indexKey = ikg.createIndexRowKey(key, valueBytes);

			ImmutableBytesWritable index = new ImmutableBytesWritable(
					Bytes.add(new byte[]{(byte) count}, indexKey));

			context.write(index, value);

			count++;
		}
	}
}
