package ict.ocrabase.main.java.client.bulkload;

import ict.ocrabase.main.java.client.bulkload.index.IndexFromTableMapper;
import ict.ocrabase.main.java.client.bulkload.index.IndexFromTableReducer;
import ict.ocrabase.main.java.client.bulkload.index.IndexFromTextMapper;
import ict.ocrabase.main.java.client.bulkload.index.IndexFromTextReducer;
import ict.ocrabase.main.java.client.bulkload.index.IndexSampler;
import ict.ocrabase.main.java.client.bulkload.index.NoKeyTextIndexMapper;
import ict.ocrabase.main.java.client.bulkload.noindex.HFileOutput;
import ict.ocrabase.main.java.client.bulkload.noindex.IncUDRowkeyMapper;
import ict.ocrabase.main.java.client.bulkload.noindex.IncrementalRowkeyMapper;
import ict.ocrabase.main.java.client.bulkload.noindex.KVReducer;
import ict.ocrabase.main.java.client.bulkload.noindex.KeyValueInput;
import ict.ocrabase.main.java.client.bulkload.noindex.UserDefinedRowkeyMapper;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
/**
 * The main class for MapReduce job
 * @author gu
 *
 */
public class MRImport {
	public static final String Name = "MRImport";

	private TableInfo t;

	private Configuration config;
	private FileSystem fs;

	private List<Path> files;
	
	private Path tempDir;
	private Path outputDir;
	
	private String type;
	private boolean insert;
	
	private int MaxID;
	private int firstLen;
	private int secondLen;
	
//	public static long lines;
//	public static long size;
	
//	public static long midTime;
	private int reduceNum;
	
	private static Job runJob;
	
	private volatile float progress = 0;
	
	private volatile long lines;
	private volatile long size;
	private volatile long outSize;

	static final Log LOG = LogFactory.getLog(MRImport.class);


	/**
	 * Do nothing
	 */
	public MRImport(){}
	
	/**
	 * Analyze input arguments, and init FileSystem and HBaseAdmin etc.
	 * @param type the import type
	 * @param dir the input source dir
	 * @param isInsert true if it insert data into an exist table
	 * @param tab the table structure
	 * @throws IOException
	 */
	public MRImport(String type,
			String dir, boolean isInsert, Configuration conf, Path tempDir) throws IOException {
		if(type.startsWith("u")){
			String[] temp = type.split(" ");
			this.type = temp[0];
			this.reduceNum = Integer.parseInt(temp[1]);
		}else if(type.startsWith("iu")){
			this.type = "incUD";
		}else if(type.startsWith("i")){
			String[] temp = type.split(" ");
			this.type = temp[0];
			String[] token = temp[1].split(":", 3);
			this.firstLen = Integer.parseInt(token[0]);
			this.secondLen = Integer.parseInt(token[1]);
			this.MaxID = Integer.parseInt(token[2]);
			
			if(temp.length == 3)
				this.reduceNum = Integer.valueOf(temp[2]);
		}else if(type.startsWith("addIndex")){
			String[] temp = type.split(" ");
			this.type = temp[0];
			this.reduceNum = Integer.parseInt(temp[1]);
		}
		
		insert = isInsert;
		
		config = new Configuration(conf);
		
		fs = FileSystem.get(config);
		
		t = new TableInfo(config.get(ImportConstants.BULKLOAD_DATA_FORMAT));
		
//		tempDir = new Path(fs.getUri().toString(), "/"+t.getTableName()
//				+ "_tmp");
		this.tempDir = tempDir;
		outputDir = new Path(tempDir,"output");
		
		if(dir != null)
			files = BulkLoadUtil.scanDirectory(fs, new Path(dir));
	}

	/**
	 * Init local MapReduce and FileSystem
	 * @param type the import type
	 * @param dir the input source dir
	 * @param isInsert true if it insert data into an exist table
	 * @param tab the table structure
	 * @throws IOException
	 */
	public static MRImport LocalMRImport(String type, String dir, boolean isInsert, Configuration conf) throws IOException {
		MRImport mr = new MRImport();
		if(type.startsWith("u")){
			String[] temp = type.split(" ");
			mr.type = temp[0];
			mr.reduceNum = 1;
		}else if(type.startsWith("iu")){
			mr.type = "incUD";
		}else if(type.startsWith("i")){
			String[] temp = type.split(" ");
			mr.type = temp[0];
			String[] token = temp[1].split(":", 3);
			mr.firstLen = Integer.parseInt(token[0]);
			mr.secondLen = Integer.parseInt(token[1]);
			mr.MaxID = Integer.parseInt(token[2]);
		}
		mr.insert = isInsert;
		mr.config = new Configuration(conf);
		mr.config.addResource("core-default.xml");
		mr.config.addResource("mapred-default.xml");
		mr.config.addResource("hdfs-default.xml");
		mr.config.reloadConfiguration();
		mr.fs = FileSystem.getLocal(mr.config);
		mr.t = new TableInfo(mr.config.get(ImportConstants.BULKLOAD_DATA_FORMAT));
		mr.config.set(ImportConstants.BULKLOAD_DATA_FORMAT, mr.t.toString());
		mr.config.set("sampler.outputdir", BulkLoad.PRE_TEST_DIR+"/sampler");
		
		mr.tempDir = new Path(BulkLoad.PRE_TEST_DIR);
		mr.outputDir = new Path(mr.tempDir,"output");
		
		mr.files = BulkLoadUtil.scanDirectory(mr.fs, new Path(dir));
		
		return mr;
	}
	
	public float getProgress(){
		return progress;
	}
	
	public long getLinesNum(){
		return lines;
	}
	
	public long getInputSize(){
		return size;
	}
	
	public long getOutputSize(){
		return outSize;
	}

	///////////////////////////////////////////
	//IncrementalRowkeyJob
	///////////////////////////////////////////
	
	/**
	 * Incremental rowkey, generate by program
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private void runIncrementalRowkeyJob() throws IOException, InterruptedException,
			ClassNotFoundException {
		config.setInt("bulkload.separator",(int) (char)t.getSeparator());
		config.setInt("incremental.max.id", MaxID);
		config.setInt("incremental.rowkey.first", firstLen);
		config.setInt("incremental.rowkey.second", secondLen);
		Job job = new Job(config, "IncrementalRowkeyImport");
		job.setJarByClass(MRImport.class);

		Path fileArray[] = files.toArray(new Path[0]);
		FileInputFormat.setInputPaths(job, fileArray);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setMapperClass(IncrementalRowkeyMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KeyValue[].class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(HFileOutput.class);
		
		
		job.submit();
		runJob = job;
	}
	
	/**
	 * Incremental rowkey, main function
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private void runIncrementalRowkeyLoad() throws IOException, InterruptedException, ClassNotFoundException {	
		runIncrementalRowkeyJob();
	}
	
	/**
	 * Import no key text with index
	 * @throws IOException
	 * @throws IllegalStateException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private void runNoKeyTextIndexJob() throws IOException, IllegalStateException, ClassNotFoundException, InterruptedException {
		config.set("mapreduce.totalorderpartitioner.path", tempDir.toString()+"/"+t.getTableName()+"_partition.lst");
		config.setInt("bulkload.separator",(int) (char)t.getSeparator());
		config.setInt("incremental.max.id", MaxID);
		config.setInt("incremental.rowkey.first", firstLen);
		config.setInt("incremental.rowkey.second", secondLen);
		Job job = new Job(config, "NoKeyTextIndexImport");
		job.setJarByClass(MRImport.class);
		
		Path fileArray[] = files.toArray(new Path[0]);
		FileInputFormat.setInputPaths(job, fileArray);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(NoKeyTextIndexMapper.class);
		job.setReducerClass(IndexFromTextReducer.class);
		job.setNumReduceTasks(reduceNum);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KeyValue[].class);


		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputFormatClass(HFileOutput.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		
		IndexSampler s = new IndexSampler(t.toString(), fileArray, job);
		while(!s.isComplete()){
			progress = (float)(0.2*s.getProgress());
			Thread.sleep(1000);
		}
		
		if(s.isSuccessful())
			s.writePartitionFile();
		else
			throw new IOException("Sampler faild! Please check the jobtracker web page for more detail.");

		job.submit();
		runJob = job;
	}
	
	/**
	 * Insert no key text with index
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private void runNoKeyTextIndexInsertJob() throws IOException, InterruptedException, ClassNotFoundException {
		config.set("mapreduce.totalorderpartitioner.path", tempDir.toString()+"/"+t.getTableName()+"_partition.lst");
		config.setInt("bulkload.separator",(int) (char)t.getSeparator());
		config.setInt("incremental.max.id", MaxID);
		config.setInt("incremental.rowkey.first", firstLen);
		config.setInt("incremental.rowkey.second", secondLen);
		Job job = new Job(config, "NoKeyTextIndexInsert");
		job.setJarByClass(MRImport.class);
		
		Path fileArray[] = files.toArray(new Path[0]);
		FileInputFormat.setInputPaths(job, fileArray);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(NoKeyTextIndexMapper.class);
		job.setReducerClass(IndexFromTextReducer.class);
		job.setNumReduceTasks(reduceNum);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KeyValue[].class);


		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputFormatClass(HFileOutput.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);

		job.submit();
		runJob = job;
	}
	
	/**
	 * Main function of no key text import with index
	 * @throws IllegalStateException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private void runNoKeyTextIndexLoad() throws IllegalStateException, IOException, ClassNotFoundException, InterruptedException {
		if(insert)
			runNoKeyTextIndexInsertJob();
		else
			runNoKeyTextIndexJob();
	}
	
	///////////////////////////////////////////
	//UserDefinedRowkeyJob
	///////////////////////////////////////////
	
	/**
	 * User define the rowkey
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private void runUserDefinedRowkeyJob() throws IOException, InterruptedException, ClassNotFoundException{
		config.set("mapreduce.totalorderpartitioner.path", tempDir.toString()+"/"+t.getTableName()+"_partition.lst");
		config.setInt("bulkload.separator",(int) (char)t.getSeparator());
		
		
/////////////////////////////////////////qihouliang-test////////////////////////////////////////
		config.set("fs.default.name","hdfs://data12:9000/");
/////////////////////////////////////////qihouliang-test////////////////////////////////////////
		
		
		Job job = new Job(config, "UserDefinedRowkeyImport");
		job.setJarByClass(MRImport.class);

		Path[] fileArray = files.toArray(new Path[0]);
		FileInputFormat.setInputPaths(job, fileArray);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setMapperClass(UserDefinedRowkeyMapper.class);
		job.setReducerClass(KVReducer.class);
		job.setNumReduceTasks(reduceNum);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KeyValue[].class);

		job.setInputFormatClass(KeyValueInput.class);
		
		job.setOutputFormatClass(HFileOutput.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		Sampler s = new Sampler(fileArray, job);
		while(!s.isComplete()){
			progress = (float)(0.2*s.getProgress());
			Thread.sleep(1000);
		}
		
//		if(s.isSuccessful()){
//			if(!s.writePartitionFile()){
//				job.setNumReduceTasks(1);
//				job.setPartitionerClass(HashPartitioner.class);
//			}
//		} else
//			throw new IOException("Sampler faild! Please check the jobtracker web page for more detail.");

		job.submit();
		runJob = job;

	}
	
	/**
	 * Insert into a table that already exists
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private void runUserDefinedRowkeyInsertJob() throws IOException, InterruptedException, ClassNotFoundException{
		config.set("mapreduce.totalorderpartitioner.path", tempDir.toString()+"/"+t.getTableName()+"_partition.lst");
		config.setInt("bulkload.separator",(int) (char)t.getSeparator());
		Job job = new Job(config, "UserDefinedRowkeyInsertImport");
		job.setJarByClass(MRImport.class);

		Path fileArray[] = files.toArray(new Path[0]);
		FileInputFormat.setInputPaths(job, fileArray);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setMapperClass(UserDefinedRowkeyMapper.class);
		job.setReducerClass(KVReducer.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KeyValue[].class);
		
		job.setInputFormatClass(KeyValueInput.class);
		
		job.setOutputFormatClass(HFileOutput.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		
		job.setNumReduceTasks(reduceNum);

		job.submit();
		runJob = job;
	}
	
	/**
	 * Main function of userdefined job 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private void runUserDefinedRowkeyLoad() throws IOException, InterruptedException, ClassNotFoundException{
		if(insert){
			runUserDefinedRowkeyInsertJob();
		}else{
			runUserDefinedRowkeyJob();
		}
	}
	
	///////////////////////////////////////////
	//IncUDRowkeyJob
	///////////////////////////////////////////
	
	/**
	 * User define the rowkey and the rowkey is increasing
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private void runIncUDRowkeyLoad() throws IOException, InterruptedException, ClassNotFoundException {
		runIncUDRowkeyJob();
	}

	/**
	 * User define the rowkey and the rowkey is increasing
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private void runIncUDRowkeyJob() throws IOException, InterruptedException, ClassNotFoundException {
		config.setInt("bulkload.separator",(int) (char)t.getSeparator());
		Job job = new Job(config, "IncUDRowkeyImport");
		job.setJarByClass(MRImport.class);
		Path fileArray[] = files.toArray(new Path[0]);
		FileInputFormat.setInputPaths(job, fileArray);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setMapperClass(IncUDRowkeyMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KeyValue[].class);

		job.setInputFormatClass(KeyValueInput.class);
		
		job.setOutputFormatClass(HFileOutput.class);

				
		job.submit();
		
		runJob= job;
	}

	///////////////////////////////////////////
	//IndexFromTextUDJob
	///////////////////////////////////////////
	
	/**
	 * Import data with index
	 * @return job ID
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private void runIndexFromTextJob() throws IOException, ClassNotFoundException, InterruptedException{
		config.set("mapreduce.totalorderpartitioner.path", tempDir.toString()+"/"+t.getTableName()+"_partition.lst");
		config.setInt("bulkload.separator",(int) (char)t.getSeparator());
		Job job = new Job(config, "IndexFromTextUD");
		job.setJarByClass(MRImport.class);

		Path fileArray[] = files.toArray(new Path[0]);
		FileInputFormat.setInputPaths(job, fileArray);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setMapperClass(IndexFromTextMapper.class);
		job.setReducerClass(IndexFromTextReducer.class);
		job.setNumReduceTasks(reduceNum);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KeyValue[].class);


		job.setInputFormatClass(KeyValueInput.class);
		
		job.setOutputFormatClass(HFileOutput.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
////////////////////////////////////qihouliang-test////////////////////////////
//System.out.println("t.toString()--------------->"+t.toString());
//System.out.println("job.toString()--------------->"+job.toString());
//for(int i=0;i<fileArray.length;i++){
//	System.out.println("fileArray[--------------->"+i+"]"+fileArray[i].toString());
//}
////////////////////////////////////qihouliang-test////////////////////////////
		IndexSampler s = new IndexSampler(t.toString(), fileArray, job);
		
		while(!s.isComplete()){
			progress = (float)(0.2*s.getProgress());
			Thread.sleep(1000);
		}
		
		if(s.isSuccessful())
			s.writePartitionFile();
		else
			throw new IOException("Sampler faild! Please check the jobtracker web page for more detail.");

		job.submit();
		runJob = job;
	}
	
	/**
	 * Insert data and the index into a exist table
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private void runIndexFromTextUDInsertJob() throws IOException, InterruptedException, ClassNotFoundException {
		config.set("mapreduce.totalorderpartitioner.path", tempDir.toString()+"/"+t.getTableName()+"_partition.lst");
		config.setInt("bulkload.separator",(int) (char)t.getSeparator());
		
		Job job = new Job(config, "IndexFromTextUDInsert");
		job.setJarByClass(MRImport.class);

		Path fileArray[] = files.toArray(new Path[0]);
		FileInputFormat.setInputPaths(job, fileArray);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setMapperClass(IndexFromTextMapper.class);
		job.setReducerClass(IndexFromTextReducer.class);
		job.setNumReduceTasks(reduceNum);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KeyValue[].class);


		job.setInputFormatClass(KeyValueInput.class);
		
		job.setOutputFormatClass(HFileOutput.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.submit();
		runJob = job;
		
	}
	
	/**
	 * Main function of import or insert data with index
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private void runIndexFromTextUDLoad() throws IOException, ClassNotFoundException, InterruptedException {
		if(insert)
			runIndexFromTextUDInsertJob();
		else
			runIndexFromTextJob();
	}
	
	///////////////////////////////////////////
	//IndexFromTableUDJob
	///////////////////////////////////////////
	
	/**
	 * Add index into a exist table
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void runIndexFromTableJob() throws IOException, InterruptedException, ClassNotFoundException{
		config.set("mapreduce.totalorderpartitioner.path", tempDir.toString()+"/"+t.getTableName()+"_partition.lst");
		config.set(TableInputFormat.INPUT_TABLE, t.getTableName());
		if(config.get("hbase.client.scanner.caching") == null)
			config.set(TableInputFormat.SCAN_CACHEDROWS, ImportConstants.CACHED_ROWS);
		
		Job job = new Job(config, "addIndex");
		job.setJarByClass(MRImport.class);
		
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setMapperClass(IndexFromTableMapper.class);
		job.setReducerClass(IndexFromTableReducer.class);
		job.setNumReduceTasks(reduceNum);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValueArray.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KeyValue[].class);


		job.setInputFormatClass(TableInputFormat.class);
		
		job.setOutputFormatClass(HFileOutput.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		IndexSampler s = new IndexSampler(t.toString(), job);
		while(!s.isComplete()){
			progress = (float)(0.2*s.getProgress());
			Thread.sleep(1000);
		}
		
		if(s.isSuccessful())
			s.writePartitionFile();
		else
			throw new IOException("Sampler faild! Please check the jobtracker web page for more detail.");

		job.submit();
		runJob = job;
	}
	
	
	/**
	 * Run a MapReduce job
	 * @param type the import type
	 * @param dir the input source dir
	 * @param isInsert true if it insert data into an exist table
	 * @param tab the table structure
	 * @return job
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void run() throws IOException, InterruptedException, ClassNotFoundException{
////////////////////////////////////qihouliang-test////////////////////////////
System.out.println("type------------->"+type);
System.out.println("t.getIndexPos().length------------->"+t.getIndexPos().length);
System.out.println("insert------------->"+insert);
////////////////////////////////////qihouliang-test////////////////////////////
		if (type.equals("incUD"))
			runIncUDRowkeyLoad();
		else if (type.equals("i")){
			if (t.getIndexPos().length == 0)
//				runIncrementalRowkeyLoad();
				runIncUDRowkeyLoad();
			else
				//runNoKeyTextIndexLoad();
				runIndexFromTextUDLoad();
		}
		else if (type.equals("u")) {
			if (t.getIndexPos().length == 0)
				runUserDefinedRowkeyLoad();
			else
				runIndexFromTextUDLoad();
		} else if (type.equals("addIndex")) {
			runIndexFromTableJob();
		}
		
		while (!runJob.isComplete()) {
			progress = (float) (0.2 + (runJob.mapProgress() / 2 + (runJob.
					getNumReduceTasks() != 0 ? runJob.reduceProgress() / 2 : runJob
					.mapProgress() / 2)) * 0.8);
			Thread.sleep(1000);
		}

		if (runJob.isSuccessful()) {
			Counters counter = runJob.getCounters();
			lines = counter.findCounter(
					"org.apache.hadoop.mapred.Task$Counter",
					"MAP_INPUT_RECORDS").getValue();
			size = counter.findCounter("FileSystemCounters", "HDFS_BYTES_READ")
					.getValue();
			outSize = counter.findCounter("FileSystemCounters",
					"HDFS_BYTES_WRITTEN").getValue();
			progress = (float) 1;
		} else {
			throw new IOException();
		}
	}

	/**
	 * Run a local MapReduce job
	 * @param type the import type
	 * @param dir the input source dir
	 * @param isInsert true if it insert data into an exist table
	 * @param tab the table structure
	 * @return
	 * @throws IOException
	 */
	public static boolean runPreTest(String type,String dir,boolean isInsert,Configuration conf) throws IOException{
		MRImport im = MRImport.LocalMRImport(type,dir,isInsert,conf);
		
		try {
			if (im.type.equals("incUD"))
				im.runIncUDRowkeyLoad();
			else if (im.type.equals("i"))
				im.runIncrementalRowkeyLoad();
			else if (im.type.equals("u")) {
				if (im.t.getIndexPos().length == 0)
					im.runUserDefinedRowkeyLoad();
				else
					im.runIndexFromTextUDLoad();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		while(!runJob.isComplete()){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		return runJob.isSuccessful();
	}
}
