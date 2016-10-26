package ict.ocrabase.main.java.client.bulkload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRExport {
	
	private Configuration config;
	private FileSystem fs;
	private Path outputDir;
	private Job job = null;

	/**
	 * Create a export MapReduce job
	 * @param tableName the table you want to export
	 * @param colFormat the column format of each column includes their types
	 * @param separator the separator of the output data
	 * @param outputDir where to put output data
	 * @throws IOException
	 */
	public MRExport(String tableName, Map<String, ArrayList<String>> colFormat, byte separator, String outputDir) throws IOException{
		this(new TableInfo(tableName,colFormat,separator), outputDir);
	}
	
	/**
	 * Create a export MapReduce job
	 * @param info the output data structure
	 * @param outputDir where to put output data
	 * @throws IOException
	 */
	public MRExport(String info, String outputDir) throws IOException{
		this(new TableInfo(info),outputDir);
	}
	
	/**
	 * Create a export MapReduce job
	 * @param tab the output data structure
	 * @param outputDir where to put output data
	 * @throws IOException
	 */
	public MRExport(TableInfo tab, String outputDir) throws IOException{
		config = HBaseConfiguration.create();
		config.set(ImportConstants.BULKLOAD_DATA_FORMAT, tab.toString());
		config.set(TableInputFormat.INPUT_TABLE, tab.getTableName());
		if(config.get("hbase.client.scanner.caching") == null)
			config.set(TableInputFormat.SCAN_CACHEDROWS, ImportConstants.CACHED_ROWS);
		
		fs = FileSystem.get(config);
		this.outputDir = new Path(outputDir);
		
		if(fs.exists(this.outputDir))
			fs.delete(this.outputDir, true);
	}
	
	/**
	 * 
	 * @author gu
	 *
	 */
	public static class ExportMapper extends Mapper<ImmutableBytesWritable, Result, NullWritable, Text>{
		private TableInfo table;
		private char separator;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			table = new TableInfo(context.getConfiguration().get(
					ImportConstants.BULKLOAD_DATA_FORMAT));
			separator = (char)table.getSeparator();
		}
		
		protected void map(ImmutableBytesWritable key, Result result,Mapper<ImmutableBytesWritable, Result, NullWritable, Text>.Context context) throws IOException, InterruptedException{
			StringBuffer str = new StringBuffer(Bytes.toString(key.get(), key.getOffset(), key.getLength()));
			
			for(TableInfo.ColumnInfo col : table.getColumnInfo()){
				str.append(separator);
				KeyValue kv = result.getColumnLatest(col.getFamily(), col.getQualifier());
				
				switch(col.getDataType()){
				case STRING:
					str.append(Bytes.toString(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength()));
					break;
				case LONG:
					str.append(Bytes.toLong(kv.getBuffer(), kv.getValueOffset()));
					break;
				case DOUBLE:
					str.append(Bytes.toDouble(kv.getBuffer(), kv.getValueOffset()));
					break;
				case INT:
					str.append(Bytes.toInt(kv.getBuffer(), kv.getValueOffset()));
					break;
				case BOOLEAN:
					str.append(Bytes.toBoolean(kv.getValue()));
					break;
				}
			}
			context.write(null, new Text(str.toString()));
		}
	}
	
	/**
	 * Run the job
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void run() throws IOException, InterruptedException, ClassNotFoundException{
		job = new Job(config, "Export Table");
		job.setJarByClass(MRExport.class);
		
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(ExportMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TableInputFormat.class);
		
		job.submit();
	}
	
	/**
	 * Get the progress of the job
	 * @return the progress of the job between 0 and 100
	 * @throws IOException
	 */
	public int getProgress() throws IOException{
		return (int)(job.mapProgress()*100);
	}
	
	/**
	 * Check the job is finished or not
	 * @return true if the job is finished
	 * @throws IOException
	 */
	public boolean isComplete() throws IOException{
		return job.isComplete();
	}
	
	/**
	 * Check the job is successful or not
	 * @return true if the job is successful
	 * @throws IOException
	 */
	public boolean isSuccessful() throws IOException{
		return job.isSuccessful();
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		String dataFormat = null;
		String outputDir = null;
		
		Options options = new Options();
		options.addOption("ts", "tableStructs", true, "Describe output text table structure.");
		options.addOption("o", "outputDir", true, "Where to put output text data.");
		options.addOption("h", "help", false, "Print help message.");
		
		if (args.length == 0) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("BulkLoad", options, true);
			System.exit(-1);
		}
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd;

		try {
			cmd = parser.parse(options, args);
			
			if(cmd.hasOption("ts"))
				dataFormat = cmd.getOptionValue("ts");
			else
				throw new IOException("Please input output data structure.");
			
			String tableName = new TableInfo(dataFormat).getTableName();
			
			outputDir = cmd.getOptionValue("o", "/"+tableName+"_output");
		} catch(ParseException e){
			e.printStackTrace();
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("BulkLoad", options, true);
			System.exit(-1);
		} catch(IOException e){
			throw e;
		}
		
		MRExport ex = new MRExport(dataFormat, outputDir);
		ex.run();
		
		while(!ex.isComplete()){
			System.out.println(ex.getProgress());
			Thread.sleep(3000);
		}
		System.out.println(ex.getProgress());

	}

}
