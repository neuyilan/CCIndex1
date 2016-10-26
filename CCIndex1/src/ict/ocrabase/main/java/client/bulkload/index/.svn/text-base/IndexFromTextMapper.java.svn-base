package ict.ocrabase.main.java.client.bulkload.index;

import ict.ocrabase.main.java.client.bulkload.BulkLoadUtil;
import ict.ocrabase.main.java.client.bulkload.ImportConstants;
import ict.ocrabase.main.java.client.bulkload.TableInfo;
import ict.ocrabase.main.java.client.index.IndexKeyGenerator;
import ict.ocrabase.main.java.client.index.SimpleIndexKeyGenerator;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Used for importing data and index into the table, transfer the rowkey to index table rowkey
 * @author gu
 *
 */
public class IndexFromTextMapper extends
		Mapper<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text> {

	private TableInfo table;
	private int[] indexPos;
	private IndexKeyGenerator ikg;

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
	}
	
	protected void map(ImmutableBytesWritable key, Text value,
			Mapper<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text>.Context context) throws IOException, InterruptedException {
		byte[] rowkey = new byte[1 + key.getLength()];
		Bytes.putByte(rowkey, 0, (byte) 0);
		Bytes.putBytes(rowkey, 1, key.get(), key.getOffset(),
				key.getLength());
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
			byte[] indexKey = ikg.createIndexRowKey(key.get(), valueBytes);

			ImmutableBytesWritable index = new ImmutableBytesWritable(
					Bytes.add(new byte[]{(byte) count}, indexKey));

			context.write(index, value);

			count++;
		}
	}
}

