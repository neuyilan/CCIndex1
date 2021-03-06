package ict.ocrabase.main.java.client.bulkload.index;

import ict.ocrabase.main.java.client.bulkload.BulkLoadUtil;
import ict.ocrabase.main.java.client.bulkload.ImportConstants;
import ict.ocrabase.main.java.client.bulkload.Sampler;
import ict.ocrabase.main.java.client.bulkload.TableInfo;
import ict.ocrabase.main.java.client.bulkload.TableInfo.ColumnInfo;
import ict.ocrabase.main.java.client.index.IndexKeyGenerator;
import ict.ocrabase.main.java.client.index.SimpleIndexKeyGenerator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * Sampling for the index table rowkey
 * @author gu
 *
 */
public class IndexSampler {
	public static class NoKeyTextSamplerMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, NullWritable>{
		private double freq;
		private long records;
		private long kept;
		private NullWritable nullValue;
		private TableInfo tab;
		private int[] indexPos;
		private IndexKeyGenerator ikg;
		
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
		
		protected void setup(Context context) throws IOException ,InterruptedException {
			super.setup(context);
			int firstLen = context.getConfiguration().getInt(
					"incremental.rowkey.first", 15);
			int secondLen = context.getConfiguration().getInt(
					"incremental.rowkey.second", 15);
			firstZero = makeZero(firstLen);
			secondZero = makeZero(secondLen);
			taskID = context.getTaskAttemptID().getTaskID().getId()
					+ context.getConfiguration().getInt("incremental.max.id", 0);

			tab = new TableInfo(context.getConfiguration().get(
					"sampler.data.format"));
			
			freq = Double.valueOf(context.getConfiguration().get("sampler.freq", "0.01"));
			indexPos = tab.getIndexPos();
			records = 0l;
			kept = 0l;
			nullValue = NullWritable.get();
			
			try {
				Constructor<?> cons = context.getConfiguration().getClass("bulkload.indexKeyGenerator", SimpleIndexKeyGenerator.class).getConstructor();
				ikg = (IndexKeyGenerator)cons.newInstance();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		private byte[] getRowkey() {
			return Bytes.toBytes(firstZero.substring(sizeOfInt(taskID)) + taskID + secondZero.substring(sizeOfInt((int)records)) + records);
		}
		
		protected void map(LongWritable lineNum, Text value, Mapper<LongWritable,Text,ImmutableBytesWritable,NullWritable>.Context context) throws IOException ,InterruptedException {
			records++;
			if((double) kept / records < freq){
				kept++;
				byte[] key = getRowkey();
				byte[] rowkey = new byte[1 + key.length];
				Bytes.putByte(rowkey, 0, (byte) 0);
				Bytes.putBytes(rowkey, 1, key, 0, key.length);
				ImmutableBytesWritable indexRowkey = new ImmutableBytesWritable(
						rowkey);
				
				context.write(indexRowkey, nullValue);
				byte[] line = value.getBytes();
				int lineLen = value.getLength();
				Integer[] split = BulkLoadUtil.dataSplit(tab.getSeparator(), line,
						lineLen);
				int count = 1;
				byte[] valueBytes;
				for (int p : indexPos) {
					if(split[p]+1 == split[p+1]){
						count++;
						continue;
					}
					TableInfo.ColumnInfo col = tab.getColumnInfo(p);
					valueBytes = BulkLoadUtil.convertToValueBytes(col.getDataType(), line, split[p]+1, split[p+1]-split[p]-1);
					byte[] indexKey = ikg.createIndexRowKey(key, valueBytes);
					 
					ImmutableBytesWritable index = new ImmutableBytesWritable(
							Bytes.add(new byte[]{(byte) count}, indexKey));

					context.write(index, nullValue);
					count++;
				}
			}
		}
	}
	
	/**
	 * Get some samples from text input data, also we need to convert the rowkey to index table rowkey
	 * @author gu
	 *
	 */
	public static class TextSamplerMapper extends Mapper<ImmutableBytesWritable, Text, ImmutableBytesWritable, NullWritable>{
		
		private double freq;
		private long records;
		private long kept;
		private NullWritable nullValue;
		private TableInfo tab;
		private int[] indexPos;
		private IndexKeyGenerator ikg;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			freq = Double.valueOf(context.getConfiguration().get("sampler.freq", "0.01"));
			tab = new TableInfo(context.getConfiguration().get("sampler.data.format"));
			indexPos = tab.getIndexPos();
			records = 0l;
			kept = 0l;
			nullValue = NullWritable.get();
			
			try {
				Constructor<?> cons = context.getConfiguration().getClass("bulkload.indexKeyGenerator", SimpleIndexKeyGenerator.class).getConstructor();
				ikg = (IndexKeyGenerator)cons.newInstance();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
		protected void map(ImmutableBytesWritable key, Text value, 
                Mapper<ImmutableBytesWritable, Text, ImmutableBytesWritable, NullWritable>.Context context) throws IOException, InterruptedException {
			records++;
			if((double) kept / records < freq){
				kept++;
				
				byte[] rowkey = new byte[1 + key.getLength()];
				Bytes.putByte(rowkey, 0, (byte) 0);
				Bytes.putBytes(rowkey, 1, key.get(), key.getOffset(),
						key.getLength());
				ImmutableBytesWritable indexRowkey = new ImmutableBytesWritable(
						rowkey);
				
				context.write(indexRowkey, nullValue);
				byte[] line = value.getBytes();
				int lineLen = value.getLength();
				Integer[] split = BulkLoadUtil.dataSplit(tab.getSeparator(), line,
						lineLen);
				int count = 1;
				byte[] valueBytes;
				for (int p : indexPos) {
					if(split[p]+1 == split[p+1]){
						count++;
						continue;
					}
					TableInfo.ColumnInfo col = tab.getColumnInfo(p);
					valueBytes = BulkLoadUtil.convertToValueBytes(col.getDataType(), line, split[p]+1, split[p+1]-split[p]-1);
					byte[] indexKey = ikg.createIndexRowKey(key.get(), valueBytes);
					 
					ImmutableBytesWritable index = new ImmutableBytesWritable(
							Bytes.add(new byte[]{(byte) count}, indexKey));

					context.write(index, nullValue);
					count++;
				}
			}
		}
		
	}
	
	/**
	 * Get some samples from the exist table, also we need to convert the rowkey to index table rowkey
	 * @author gu
	 *
	 */
	public static class TableSamplerMapper extends Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, NullWritable>{
		private double freq;
		private long records;
		private long kept;
//		private long maxScan;
		private NullWritable nullValue;
		private TableInfo tab;
		private IndexKeyGenerator ikg;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			freq = Double.valueOf(context.getConfiguration().get("sampler.freq", "0.01"));
			tab = new TableInfo(context.getConfiguration().get("sampler.data.format"));
			records = 0l;
			kept = 0l;
//			maxScan = context.getConfiguration().getLong("sampler.max.scan", 1000000);
			nullValue = NullWritable.get();
			
			try {
				Constructor<?> cons = context.getConfiguration().getClass("bulkload.indexKeyGenerator", SimpleIndexKeyGenerator.class).getConstructor();
				ikg = (IndexKeyGenerator)cons.newInstance();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		protected void map(ImmutableBytesWritable key, Result value, 
                Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, NullWritable>.Context context) throws IOException, InterruptedException {
			records++;
			if((double) kept / records < freq){
				kept++;
				
				int count = 1;
				for(TableInfo.ColumnInfo col : tab.getColumnInfo()){
					KeyValue kv = value.getColumnLatest(col.getFamily(), col.getQualifier());
					if(kv == null || kv.getValueLength() == 0){
						count ++;
						continue;
					}
					byte[] rowkey = new byte[key.getLength()];
					System.arraycopy(key.get(), key.getOffset(), rowkey, 0, key.getLength());
					byte[] indexKey = ikg.createIndexRowKey(rowkey, kv.getValue());
					
					ImmutableBytesWritable result = new ImmutableBytesWritable(
							Bytes.add(new byte[]{(byte) count}, indexKey));

					context.write(result, nullValue);
					count++;
				}
			}
		}
		
		public void run(Context context) throws IOException, InterruptedException {
		    setup(context);
		    while (context.nextKeyValue()) {
		      map(context.getCurrentKey(), context.getCurrentValue(), context);
		    }
		    cleanup(context);
		  }
	}
	
	/**
	 * Split all the samples into a few region, get all the region split keys
	 * @author gu
	 *
	 */
	public static class SamplerReducer extends Reducer<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, NullWritable>{
		
		private TableInfo tab;
		
		protected void setup(Context context){
			tab = new TableInfo(context.getConfiguration().get("sampler.data.format"));
		}
		
		@SuppressWarnings("unchecked")
		public void run(Context context) throws IOException, InterruptedException {
			setup(context);
			
			NullWritable nullValue = NullWritable.get();
			ArrayList<ImmutableBytesWritable> sampleList = new ArrayList<ImmutableBytesWritable>();
			while (context.nextKey()) {
				sampleList.add(ReflectionUtils.copy(context.getConfiguration(),
                        context.getCurrentKey(), null));
			}
			
//			for(KEYIN sample : sampleList)
//				context.write((KEYOUT)sample, (VALUEOUT)nullValue);
			
			RawComparator<ImmutableBytesWritable> comparator =
			      (RawComparator<ImmutableBytesWritable>) context.getSortComparator();
			
			ImmutableBytesWritable[] samples = sampleList.toArray(new ImmutableBytesWritable[0]);
			int tableNum = tab.getIndexPos().length;
			int numPartitions = context.getConfiguration().getInt("sampler.reduceNum", 10) - tableNum;
			
			
			Path dst = new Path(TotalOrderPartitioner.getPartitionFile(context.getConfiguration()));
		    FileSystem fs = dst.getFileSystem(context.getConfiguration());
		    if (fs.exists(dst)) {
		      fs.delete(dst, false);
		    }
//		    SequenceFile.Writer writer = SequenceFile.createWriter(fs, 
//		      context.getConfiguration(), dst, context.getMapOutputKeyClass(), NullWritable.class);
			
		    ArrayList<ImmutableBytesWritable> sam = new ArrayList<ImmutableBytesWritable>();
		    if(samples.length <= numPartitions){
		    	for(ImmutableBytesWritable sample : samples){
		    		sam.add(sample);
		    	}
		    	Random r = new Random();
		    	while(sam.size()<numPartitions-1){
		    		byte[] randomBytes = new byte[r.nextInt(20)+1];
		    		r.nextBytes(randomBytes);
		    		ImmutableBytesWritable imm = new ImmutableBytesWritable(randomBytes);
		    		if(sam.contains(imm)){
		    			continue;
		    		}
		    		sam.add(imm);
		    	}
		    	
		    } else {
			    float stepSize = samples.length/ (float) numPartitions;
			    int last = -1;
			    for(int i = 1; i < numPartitions; ++i) {
			      int k = Math.round(stepSize * i);
			      while (last >= k && comparator.compare(samples[last], samples[k]) == 0) {
			        ++k;
			      }
			      sam.add(samples[k]);
			      last = k;
			    }
		    }
			for (int i = 1; i <= tableNum; i++) {
				byte[] s = { (byte) i };
				sam.add(new ImmutableBytesWritable(s));
			}
			Collections.sort(sam, comparator);
			for(ImmutableBytesWritable s : sam)
				context.write(s, nullValue);

		    cleanup(context);
		}
	}
	
	private FileSystem fs;
	private Job samplerJob;
	private String outPath;
	private String partitionFile;
	
	/**
	 * Construct a text index sampler
	 * @param info the data format string
	 * @param files the data files you want to sample
	 * @param job the import data job
	 * @throws IOException
	 * @throws IllegalStateException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public IndexSampler(String info, Path[] files, Job job) throws IOException, IllegalStateException, ClassNotFoundException, InterruptedException{
		Configuration config = new Configuration(job.getConfiguration());
		fs = FileSystem.get(config);
		int reduceNum = job.getNumReduceTasks();
		
		long total = getTotalSize(files);
		long avg = getAvgSize(files);
		long len = total/avg;
		
		double freq = (double)(reduceNum*1000)/(double)len;
		
		config.set("sampler.freq", String.valueOf(freq));
		config.setInt("sampler.reduceNum",job.getNumReduceTasks());
		config.set("sampler.data.format", info);
		runTextSamplerJob(config, files, job);
	}
	
	/**
	 * Construct a table index sampler
	 * @param info the data format string
	 * @param job the import data job
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws IllegalStateException
	 * @throws ClassNotFoundException
	 */
	public IndexSampler(String info, Job job) throws IOException, InterruptedException, IllegalStateException, ClassNotFoundException{
		Configuration config = new Configuration(job.getConfiguration());
		fs = FileSystem.get(config);
		int reduceNum = job.getNumReduceTasks();
		TableInfo tab = new TableInfo(info);
		
		long len = getLenOfTable(job, tab.getTableName());
		double freq = (double)(reduceNum*100)/(double)len;
		
		config.set("sampler.freq", String.valueOf(freq));
		config.setInt("sampler.reduceNum",job.getNumReduceTasks());
		config.set("sampler.data.format", info);
		runTableSamplerJob(config,job);
	}

	/**
	 * Estimate the number of rows in the table
	 * @param job the import data job
	 * @return the estimate number
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private long getLenOfTable(Job job,String tableName) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		InputFormat<ImmutableBytesWritable, Result> inf = (InputFormat<ImmutableBytesWritable, Result>) ReflectionUtils.newInstance(TableInputFormat.class, conf);
		
		List<InputSplit> splits = inf.getSplits(job);
		
		long total = countTotalLen(new Path("/hbase/"+tableName));
		
		int count = 0;
		long samSize = 0;
		for(int i=0;i<splits.size() && count < 10000;i++){
			TaskAttemptContext samplingContext = new TaskAttemptContext(job
					.getConfiguration(), new TaskAttemptID());
			RecordReader<ImmutableBytesWritable, Result> reader = inf
					.createRecordReader(splits.get(i), samplingContext);
			reader.initialize(splits.get(i), samplingContext);
			while (reader.nextKeyValue() && count < 10000) {
				count ++;
				Result rst = reader.getCurrentValue();
				List<KeyValue> kvs = rst.list();
				for(KeyValue kv : kvs)
					samSize += kv.getLength();
			}
		}
		long avg = samSize/count;
		
		return total/avg;
	}
	
	/**
	 * Get the total size of the files in a directory
	 * @param dir the directory you want to scan
	 * @return the total size of the files
	 * @throws IOException
	 */
	private long countTotalLen(Path dir) throws IOException{
		long total = 0;
		FileStatus[] fList = fs.listStatus(dir);
		int fileNum = fList.length;
		for (int i = 0; i < fileNum; i++) {
			if(fList[i].getPath().getName().matches("(_|\\.).*"))
				continue;
			
			if (fList[i].isDir())
				total += countTotalLen(fList[i].getPath());
			else
				total += fList[i].getLen();
		}
		return total;
	}

	/**
	 * Run the text sampler job
	 * @param config the import data job configuration
	 * @param files the data files you want to sample
	 * @param job the import data job
	 * @throws IOException
	 * @throws IllegalStateException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private void runTextSamplerJob(Configuration config, Path[] files, Job job) throws IOException, IllegalStateException, ClassNotFoundException, InterruptedException {
		samplerJob = new Job(config, "Sampler");
		samplerJob.setJarByClass(Sampler.class);
		FileInputFormat.setInputPaths(samplerJob, files);
		
		outPath = config.get("sampler.outputdir", "/_sampler");
		String tableName = config.get(ImportConstants.BULKLOAD_DATA_FORMAT, "table_without_name").split(",",2)[0];
		long timestamp = System.currentTimeMillis(); 
		while(fs.exists(new Path(outPath+"_"+tableName+"_"+timestamp))){
			timestamp ++;
 		}
		outPath = outPath+"_"+tableName+"_"+timestamp;
		
		partitionFile = TotalOrderPartitioner.getPartitionFile(config);
		
		FileOutputFormat.setOutputPath(samplerJob, new Path(outPath));
		
		if(job.getMapperClass().equals(IndexFromTextMapper.class))
			samplerJob.setMapperClass(TextSamplerMapper.class);
		else
			samplerJob.setMapperClass(NoKeyTextSamplerMapper.class);
		
		samplerJob.setReducerClass(SamplerReducer.class);
		samplerJob.setNumReduceTasks(1);
		
		samplerJob.setMapOutputKeyClass(ImmutableBytesWritable.class);
		samplerJob.setMapOutputValueClass(NullWritable.class);
		samplerJob.setOutputKeyClass(ImmutableBytesWritable.class);
		samplerJob.setOutputValueClass(NullWritable.class);
		
		samplerJob.setInputFormatClass(job.getInputFormatClass());
		samplerJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		samplerJob.setPartitionerClass(HashPartitioner.class);
		
		samplerJob.submit();
	}
	
	/**
	 * Run adding index sampler job
	 * @param config the import data job configuration
	 * @param job the import data job
	 * @throws IOException
	 * @throws IllegalStateException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private void runTableSamplerJob(Configuration config, Job job) throws IOException, IllegalStateException, ClassNotFoundException, InterruptedException {
		TableInfo tab = new TableInfo(config.get("sampler.data.format"));
		Scan scan = new Scan();
		scan.setCaching(1000);
		scan.setCacheBlocks(false);
		for(ColumnInfo col : tab.getColumnInfo()){
			scan.addColumn(col.getFamily(), col.getQualifier());
		}
		
		config.set(TableInputFormat.SCAN,BulkLoadUtil.convertScanToString(scan));
		samplerJob = new Job(config, "Sampler");
		samplerJob.setJarByClass(Sampler.class);
		
		outPath = config.get("sampler.outputdir", "/_sampler");
		String tableName = config.get(ImportConstants.BULKLOAD_DATA_FORMAT, "table_without_name").split(",",2)[0];
		long timestamp = System.currentTimeMillis(); 
		while(fs.exists(new Path(outPath+"_"+tableName+"_"+timestamp))){
			timestamp ++;
 		}
		outPath = outPath+"_"+tableName+"_"+timestamp;
		
		partitionFile = TotalOrderPartitioner.getPartitionFile(config);
		
		FileOutputFormat.setOutputPath(samplerJob, new Path(outPath));
		samplerJob.setMapperClass(TableSamplerMapper.class);
		samplerJob.setReducerClass(SamplerReducer.class);
		samplerJob.setNumReduceTasks(1);
		
		samplerJob.setMapOutputKeyClass(ImmutableBytesWritable.class);
		samplerJob.setMapOutputValueClass(NullWritable.class);
		samplerJob.setOutputKeyClass(ImmutableBytesWritable.class);
		samplerJob.setOutputValueClass(NullWritable.class);
		
		samplerJob.setInputFormatClass(job.getInputFormatClass());
		samplerJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		samplerJob.setPartitionerClass(HashPartitioner.class);
		
		samplerJob.submit();
		
	}
	
	/**
	 * Get sampler progress
	 * @return progress
	 * @throws IOException
	 */
	public float getProgress() throws IOException{
		return (float) (0.5*samplerJob.mapProgress() + 0.5*samplerJob.reduceProgress());
	}
	
	/**
	 * Check if sampler is finished or not
	 * @return true if sampler is finished
	 * @throws IOException
	 */
	public boolean isComplete() throws IOException{
		return samplerJob.isComplete();
	}
	
	/**
	 * Check if sampler is successful or not
	 * @return true if sampler is successful
	 * @throws IOException
	 */
	public boolean isSuccessful() throws IOException{
		return samplerJob.isSuccessful();
	}
	
	/**
	 * Write the partition file and cleanup work dir
	 * @throws IOException
	 */
	public void writePartitionFile() throws IOException{
		fs.rename(new Path(outPath,"part-r-00000"), new Path(partitionFile));
		fs.delete(new Path(outPath), true);
	}

	/**
	 * Estimate the total size of the input files
	 * @param files the input files
	 * @return the total size
	 * @throws IOException
	 */
	private long getTotalSize(Path[] files) throws IOException{
		long size = 0;
		for(Path file : files){
			FileStatus[] s = fs.listStatus(file);
			size += s[0].getLen();
		}
		return size;
	}
	
	/**
	 * Estimate the size of a single line in the input files
	 * @param files the input files
	 * @return the size of a single line in the input files
	 * @throws IOException
	 */
	private long getAvgSize(Path[] files) throws IOException{
		long size = 0;
		int line = 0;
		for(Path file : files){
			FSDataInputStream hdfsInStream = fs.open(file);
			BufferedReader reader = new BufferedReader(new InputStreamReader(hdfsInStream));
			String l;
			while((l = reader.readLine()) != null && line<=10000){
				line++;
				size += l.length();
			}
			if(line < 10000)
				continue;
			else
				break;
		}
		return size/line;
	}
}
