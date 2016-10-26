package ict.ocrabase.main.java.client.bulkload;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Get some samples of rowkey from the text data. The samples will partition into even regions.
 * @author gu
 *
 */
public class Sampler {
	
	public static class SamplerMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
		
		private Double freq;
		private Long records;
		private Long kept;
		private NullWritable nullValue;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			freq = Double.valueOf(context.getConfiguration().get("sampler.freq", "0.01"));
			records = 0l;
			kept = 0l;
			nullValue = NullWritable.get();
		}
		
		@SuppressWarnings("unchecked")
		protected void map(KEYIN key, VALUEIN value, 
                Context context) throws IOException, InterruptedException {
			records++;
			if((double) kept / records < freq){
				kept++;
				context.write((KEYOUT)key, (VALUEOUT) nullValue);
			}
		}
	}
	
	public static class SamplerReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
		
		@SuppressWarnings("unchecked")
		public void run(Context context) throws IOException, InterruptedException {
			setup(context);
			
			NullWritable nullValue = NullWritable.get();
			ArrayList<KEYIN> sampleList = new ArrayList<KEYIN>();
			while (context.nextKey()) {
				sampleList.add(ReflectionUtils.copy(context.getConfiguration(),
                        context.getCurrentKey(), null));
			}
			
//			for(KEYIN sample : sampleList)
//				context.write((KEYOUT)sample, (VALUEOUT)nullValue);
			
			RawComparator<KEYIN> comparator =
			      (RawComparator<KEYIN>) context.getSortComparator();
			
			KEYIN[] samples = (KEYIN[])sampleList.toArray();
			int numPartitions = context.getConfiguration().getInt("sampler.reduceNum", 10);
			
			
			Path dst = new Path(TotalOrderPartitioner.getPartitionFile(context.getConfiguration()));
		    FileSystem fs = dst.getFileSystem(context.getConfiguration());
		    if (fs.exists(dst)) {
		      fs.delete(dst, false);
		    }
//		    SequenceFile.Writer writer = SequenceFile.createWriter(fs, 
//		      context.getConfiguration(), dst, context.getMapOutputKeyClass(), NullWritable.class);
		    if(samples.length <= numPartitions){
			    Path failedPath = new Path(FileOutputFormat.getOutputPath(context),"too_few_lines");
			    System.out.println(failedPath.toString());
			    fs.createNewFile(failedPath);
			    context.write((KEYOUT)samples[0], (VALUEOUT)nullValue);
		    } else {
		    	float stepSize = samples.length/ (float) numPartitions;
		    	int last = -1;
		    	for(int i = 1; i < numPartitions; ++i) {
		    		int k = Math.round(stepSize * i);
		    		while (last >= k && comparator.compare(samples[last], samples[k]) == 0) {
		    			++k;
		    		}
		    		context.write((KEYOUT)samples[k], (VALUEOUT)nullValue);
		    		last = k;
		    	}
		    }
//		    writer.close();
		    cleanup(context);
		}
	}
	
	private FileSystem fs;
	private Job samplerJob;
	private String outPath;
	private String partitionFile;
	
	/**
	 * Construct a sampler
	 * @param files the data files you want to sample
	 * @param job the import data job
	 * @throws IOException
	 * @throws IllegalStateException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public Sampler(Path[] files, Job job) throws IOException, IllegalStateException, ClassNotFoundException, InterruptedException{
		Configuration config = new Configuration(job.getConfiguration());
		fs = FileSystem.get(config);
		int reduceNum = job.getNumReduceTasks();
		
		long total = getTotalSize(files);
		long avg = getAvgSize(files);
		long len = total/avg;
		
		double freq = (double)(reduceNum*1000)/(double)len;
		
		config.set("sampler.freq", String.valueOf(freq));
		config.setInt("sampler.reduceNum",job.getNumReduceTasks());
		runSamplerJob(config, files, job);
	}
	
	/**
	 * Run the sampler job
	 * @param config the import data job configuration
	 * @param files the data files you want to sample
	 * @param job the import data job
	 * @throws IOException
	 * @throws IllegalStateException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private void runSamplerJob(Configuration config, Path[] files, Job job) throws IOException, IllegalStateException, ClassNotFoundException, InterruptedException {
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
		samplerJob.setMapperClass(SamplerMapper.class);
		samplerJob.setReducerClass(SamplerReducer.class);
		samplerJob.setNumReduceTasks(1);
		
		samplerJob.setMapOutputKeyClass(job.getMapOutputKeyClass());
		samplerJob.setMapOutputValueClass(NullWritable.class);
		samplerJob.setOutputKeyClass(job.getMapOutputKeyClass());
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
	public boolean writePartitionFile() throws IOException{
		if(fs.exists(new Path(outPath,"too_few_lines"))){
			fs.delete(new Path(outPath), true);
			return false;
		} else {
			fs.rename(new Path(outPath,"part-r-00000"), new Path(partitionFile));
			fs.delete(new Path(outPath), true);
			return true;
		}
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
