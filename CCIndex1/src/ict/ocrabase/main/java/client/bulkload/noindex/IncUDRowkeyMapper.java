package ict.ocrabase.main.java.client.bulkload.noindex;

import ict.ocrabase.main.java.client.bulkload.BulkLoadUtil;
import ict.ocrabase.main.java.client.bulkload.ImportConstants;
import ict.ocrabase.main.java.client.bulkload.KeyValueArray;
import ict.ocrabase.main.java.client.bulkload.TableInfo;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The data has incremental rowkey, so we just need to convert text to KeyValues
 * @author gu
 *
 */
public class IncUDRowkeyMapper extends
Mapper<ImmutableBytesWritable, Text, Text, KeyValueArray> {

	private TableInfo table;
	public long count = 0;
	private Text tableName;
	private int columnNum;
	
	private int timestampPos;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		table = new TableInfo(context.getConfiguration().get(ImportConstants.BULKLOAD_DATA_FORMAT));
		this.timestampPos = table.getTimestampPos();
		tableName = new Text(table.getTableName());
		columnNum = table.getColumnInfo().size();
	}

	protected void map(ImmutableBytesWritable rowkey, Text value,
			Mapper<ImmutableBytesWritable, Text, Text, KeyValueArray>.Context context)
			throws IOException, InterruptedException {
		//*********************************qihouliang************************/
		System.out.println("*************************************");
		//*********************************qihouliang************************/
		byte[] keyBytes = rowkey.copyBytes();

		byte[] line = value.getBytes();
		int lineLength = value.getLength();
		
		Integer[] split = BulkLoadUtil.dataSplit(table.getSeparator(),line,lineLength);
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

			keyvalue = BulkLoadUtil.createKVByte(keyBytes, ci.bytes, ci.getDataType(), line, split[i]+1, split[i+1]-split[i]-1);
			
			//*****************************************qihouliang*********************/
			System.out.println("Bytes.toString(keyvalue)--------------->"+Bytes.toString(keyvalue));
			//*****************************************qihouliang*********************/
			
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

	
}
