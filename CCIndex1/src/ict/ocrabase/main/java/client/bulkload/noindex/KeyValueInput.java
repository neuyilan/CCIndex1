package ict.ocrabase.main.java.client.bulkload.noindex;
import ict.ocrabase.main.java.client.bulkload.BulkLoadUtil;
import ict.ocrabase.main.java.client.bulkload.ImportConstants;
import ict.ocrabase.main.java.client.bulkload.TableInfo;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Convert a text line into a key/value pair
 * @author gu
 *
 */
public class KeyValueInput extends FileInputFormat<ImmutableBytesWritable, Text> {

	private final LineRecordReader lineRecordReader = new LineRecordReader();
	private byte separator = (byte) '\t';
	private ImmutableBytesWritable key = null;
	private Text value = null;
	private int keyPos;
	
	@Override
	public RecordReader<ImmutableBytesWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {

		return new RecordReader<ImmutableBytesWritable, Text>(){

			@Override
			public void close() throws IOException {
				lineRecordReader.close();
			}

			@Override
			public ImmutableBytesWritable getCurrentKey() throws IOException,
					InterruptedException {
				return key;
			}

			@Override
			public Text getCurrentValue() throws IOException,
					InterruptedException {
				return value;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return lineRecordReader.getProgress();
			}

			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				lineRecordReader.initialize(split, context);
				int sep = context.getConfiguration().getInt("bulkload.separator", 9);
				separator = (byte) (char) sep;
				
				String format = context.getConfiguration().get(ImportConstants.BULKLOAD_DATA_FORMAT_WITH_KEY);
				if(format != null){
					TableInfo table = new TableInfo(format);
					keyPos = table.getKeyPos();
					if(keyPos == -1)
						keyPos = 0;
				} else {
					keyPos = 0;
				}
//				String sepStr = context.getConfiguration().get("bulkload.separator", "\t");
//			    separator = (byte) sepStr.charAt(0);
			}

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if(key == null)
					key = new ImmutableBytesWritable();
				
				if(value == null)
					value = new Text();
				
				byte[] line = null;
				int lineLen = -1;
				if(lineRecordReader.nextKeyValue()){
					line = lineRecordReader.getCurrentValue().getBytes();
					lineLen = lineRecordReader.getCurrentValue().getLength();
				}else {
					key = null;
					value = null;
					return false;
				}
				if (line == null){
					key = null;
					value = null;
					return false;
				}
				Integer[] splitPos = BulkLoadUtil.dataSplit(separator, line, lineLen);
//				int pos = findSeparator(line, 0, lineLen, separator);
				if (splitPos.length <= keyPos+1){
					key.set(line, 0, lineLen);
					value.set("");
				}else{
					int keyLen = splitPos[keyPos+1]-splitPos[keyPos]-1;
					byte[] keyBytes = new byte[keyLen];
					System.arraycopy(line, splitPos[keyPos]+1, keyBytes, 0, keyLen);
					int valLen = lineLen - keyLen - 1;
					byte[] valBytes = new byte[valLen];
					int firstLen = (splitPos[keyPos]+1)>valLen?valLen:(splitPos[keyPos]+1);
					int secondLen = lineLen - splitPos[keyPos+1] -1;
					if(firstLen != 0){
						System.arraycopy(line, 0, valBytes, 0, firstLen);
					}
					if(secondLen > 0){
						System.arraycopy(line, splitPos[keyPos+1] + 1, valBytes, firstLen, secondLen);
					}
					key.set(keyBytes);
					value.set(valBytes);
				}
				
				return true;
			}
			
		};
	}
	
	@Override
	  protected boolean isSplitable(JobContext context, Path file) {
	    CompressionCodec codec = 
	      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	    return codec == null;
	  }

}
