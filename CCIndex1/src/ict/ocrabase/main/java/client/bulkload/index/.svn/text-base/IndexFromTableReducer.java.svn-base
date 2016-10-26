package ict.ocrabase.main.java.client.bulkload.index;

import java.io.IOException;

import ict.ocrabase.main.java.client.bulkload.ImportConstants;
import ict.ocrabase.main.java.client.bulkload.KeyValueArray;
import ict.ocrabase.main.java.client.bulkload.TableInfo;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Used for adding index
 * @author gu
 *
 */
public class IndexFromTableReducer extends
Reducer<ImmutableBytesWritable, KeyValueArray, Text, KeyValueArray> {
	private TableInfo table;
	private int count = 0;
	Text tableName = null;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		table = new TableInfo(context.getConfiguration().get(
				ImportConstants.BULKLOAD_DATA_FORMAT));
	}
	
	protected void reduce(
			ImmutableBytesWritable row,
			Iterable<KeyValueArray> kvs,
			Reducer<ImmutableBytesWritable, KeyValueArray, Text, KeyValueArray>.Context context)
			throws IOException, InterruptedException {
		if(tableName == null){
			int indexPos = (int)row.get()[row.getOffset()];
			TableInfo.ColumnInfo info = table.getColumnInfo(table.getIndexPos(indexPos-1));
			tableName = new Text(info.getIndexTableName());
		}
		KeyValueArray kvList = kvs.iterator().next();
		
		context.write(tableName, kvList);
		count++;
		if(count % 10000 == 0)
			context.setStatus("Write " + count);
	}
}
