package ict.ocrabase.main.java.client.bulkload.noindex;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Doing nothing but write the input into output
 * @author gu
 *
 */
public class UserDefinedRowkeyMapper extends
		Mapper<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text> {
	
	
	public void run(Context context) throws IOException, InterruptedException {
		while (context.nextKeyValue()) {
			context.write(context.getCurrentKey(), context.getCurrentValue());
		}
	}
}
