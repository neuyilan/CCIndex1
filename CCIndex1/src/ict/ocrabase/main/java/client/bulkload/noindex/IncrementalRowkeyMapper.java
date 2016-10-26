package ict.ocrabase.main.java.client.bulkload.noindex;


import ict.ocrabase.main.java.client.bulkload.BulkLoadUtil;
import ict.ocrabase.main.java.client.bulkload.ImportConstants;
import ict.ocrabase.main.java.client.bulkload.KeyValueArray;
import ict.ocrabase.main.java.client.bulkload.TableInfo;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The original data has no rowkey, this class will add a incremental rowkey to the data
 * @author gu
 *
 */
public class IncrementalRowkeyMapper extends
		Mapper<LongWritable, Text, Text, KeyValueArray> {

	private int lineCounter;
	private int taskID;
	private String firstZero;
	private String secondZero;
	private int columnNum;
	private TableInfo table;
	private Text tableName;
	public long count = 0;
	
	private int timestampPos;
	
	final static int[] sizeTable = { 9, 99, 999, 9999, 99999, 999999, 9999999,
		99999999, 999999999, Integer.MAX_VALUE };
	private int sizeOfInt(int x) {
		for (int i = 0;; i++)
			if (x <= sizeTable[i])
				return i + 1;
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		lineCounter = 0;
		int firstLen = context.getConfiguration().getInt(
				"incremental.rowkey.first", 15);
		int secondLen = context.getConfiguration().getInt(
				"incremental.rowkey.second", 15);
		firstZero = makeZero(firstLen);
		secondZero = makeZero(secondLen);
		taskID = context.getTaskAttemptID().getTaskID().getId()
				+ context.getConfiguration().getInt("incremental.max.id", 0);

		table = new TableInfo(context.getConfiguration().get(
				ImportConstants.BULKLOAD_DATA_FORMAT));
		this.timestampPos = table.getTimestampPos();
		columnNum = table.getColumnInfo().size();
		tableName = new Text(table.getTableName());

	}

	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, Text, KeyValueArray>.Context context)
			throws IOException, InterruptedException {
		lineCounter++;

		byte[] keyBytes = getRowkey();
		System.out.println("start incremental row mapper");
		byte[] line = value.getBytes();
		int lineLength = value.getLength();

		Integer[] split = BulkLoadUtil.dataSplit(table.getSeparator(),line,lineLength);
		System.out.println("IncRowMap,split length: "+split.length+", column num:"+columnNum);
		if(split.length != columnNum+1)
			return;
		
		byte[] ts = null;
		if(this.timestampPos != -1){
			ts = Bytes.toBytes(Long.valueOf(Bytes.toString(line, split[timestampPos]+1, split[timestampPos+1]-split[timestampPos]-1)));
		}
		
		TreeSet<KeyValue> kvList = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
		int i;
		byte[] keyvalue = null;

		for (TableInfo.ColumnInfo ci : table.getColumnInfo()) {
			i = ci.getPos();
			if (split[i]+1 == split[i+1]) {
				continue;
			}
			System.out.println("create kvbyte:"+keyBytes);
			keyvalue = BulkLoadUtil.createKVByte(keyBytes, ci.bytes, ci.getDataType(), line, split[i]+1, split[i+1]-split[i]-1);
			KeyValue kv = new KeyValue(keyvalue, 0, keyvalue.length);
			if(this.timestampPos != -1){
        // kv.updateStamp(ts);
        int tsOffset = kv.getTimestampOffset();
        System.arraycopy(ts, 0, kv.getBuffer(), tsOffset, Bytes.SIZEOF_LONG);

			}
			kvList.add(kv);
		}
		context.write(tableName, new KeyValueArray(kvList));
		
		count++;
		if(count % 10000 == 0)
			context.setStatus("Write " + count);
	}

	/**
	 * Return a string of zero with the given length
	 * @param len the length of the zero string
	 * @return a string of zero with the given length
	 */
	private String makeZero(int len) {
		String zero = "";
		for (int i = 0; i < len; i++) {
			zero = zero + "0";
		}
		return zero;
	}

	/**
	 * Get a new incremental rowkey
	 * @return a new incremental rowkey
	 */
	private byte[] getRowkey() {
		StringBuffer strBuf = new StringBuffer();
		strBuf.append(firstZero.substring(sizeOfInt(taskID)));
		strBuf.append(taskID);
		strBuf.append(secondZero.substring(sizeOfInt(lineCounter)));
		strBuf.append(lineCounter);
		return Bytes.toBytes(strBuf.toString());
	}

}