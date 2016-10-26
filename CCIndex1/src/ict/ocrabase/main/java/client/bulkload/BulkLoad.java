package ict.ocrabase.main.java.client.bulkload;

import ict.ocrabase.main.java.client.bulkload.noindex.HFileOutput;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileReaderV2;
import org.apache.hadoop.hbase.io.hfile.HFileReaderV3;
import org.apache.hadoop.hbase.io.hfile.HFileWriterV3;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;

/**
 * The main interface of bulkload, include configuration and load HFiles.
 * 
 * @author gu
 * 
 */
public class BulkLoad {

	public class NoNeedBuildIndexException extends Exception{

		private static final long serialVersionUID = -304189987666851318L;
		
		public NoNeedBuildIndexException(){}
		
		public NoNeedBuildIndexException(String info){
			super(info);
		}
	}
	
	public static final String Name = "bulkload";
	
	private Configuration config;
	private FileSystem fs;
	private HBaseAdmin admin;
	private TableInfo t;
	private ArrayList<String> indexTableName;
	private String method;
	private boolean isInsert;
	private String src;

	private MRImport im = null;
	private LoadHFiles load = null;

	private volatile boolean complete = false;
	private volatile boolean success = false;
	private volatile String result = null;
	private volatile String status = null;

	private volatile long lines;
	private volatile long size;
	private volatile long outSize;

	private long startTime;
	private boolean oldBalanceValue;

	private Path tempDir;

	static final String PRE_TEST_DIR = "/tmp/preTest";

	static final Log LOG = LogFactory.getLog(BulkLoad.class);

	public BulkLoad() throws IOException {
		config = HBaseConfiguration.create();
		fs = FileSystem.get(config);
		admin = new HBaseAdmin(config);
	}
	
	/**
	 * Used for add index, read from exist table and import the data into the
	 * index table.
	 * 
	 * @param tableName
	 *            the table name of which to import data
	 * @param colFormat
	 *            the data format of import data
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws NoNeedBuildIndexException 
	 */
	public BulkLoad(String tableName, Map<String, ArrayList<String>> colFormat)
			throws IOException, InterruptedException, NoNeedBuildIndexException {
		this(new TableInfo(tableName, colFormat, (byte) 0));
	}

	/**
	 * Used for add index, read from exist table and import the data into the
	 * index table.
	 * 
	 * @param info
	 *            data format string, it contains table name, separator, column
	 *            info and index, eg.
	 *            test,test_CCT,9,f:1:String:CCINDEX:test_f_1_CCIT
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws NoNeedBuildIndexException 
	 */
	public BulkLoad(String info) throws IOException, InterruptedException, NoNeedBuildIndexException {
		this(new TableInfo(info));
	}
	
	/**
	 * Used for add index, read from exist table and import the data into the
	 * index table.
	 * 
	 * @param tableName the table to add index
	 * @param columns the columns to add index
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws NoNeedBuildIndexException 
	 */
	public BulkLoad(String tableName, String columns) throws IOException, InterruptedException, NoNeedBuildIndexException{
		this(tableName+",1,"+columns);
	}

	public BulkLoad(TableInfo tab) throws IOException, InterruptedException, NoNeedBuildIndexException{
		this(HBaseConfiguration.create(), tab);
	}
	
	/**
	 * Used for adding index, read from exist table and import the data into the
	 * index table.
	 * 
	 * @param tab
	 *            the table info, it contains table name, separator, column info
	 *            and index etc.
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws NoNeedBuildIndexException 
	 */
	public BulkLoad(Configuration conf, TableInfo tab) throws IOException, InterruptedException, NoNeedBuildIndexException {
		startTime = System.currentTimeMillis();
//		 init();
		config = conf;
		fs = FileSystem.get(config);
		admin = new HBaseAdmin(config);
		
		// Whether the table has data
		admin.enableTable(tab.getTableName());
		HTable table = new HTable(config, tab.getTableName());
		ResultScanner scanner = table.getScanner(new Scan());
		if (scanner.next() == null){
			admin.disableTable(tab.getTableName());
			throw new NoNeedBuildIndexException("Table "+tab.getTableName()+" is empty, no need to call BulkLoad to buid index.");
		}
		
    // stop balance table UNDEFINED
    oldBalanceValue = admin.setBalancerRunning(false, false);
    // admin.setRSAutoSplit(false);

		tab.addIndexInfo(config);
//		tab.addDataType(config);
		t = tab;

		// write all index table information to config
		t.writeIndexToConfig(config);

		indexTableName = getAllAddIndexTableName(t);

		tempDir = new Path(fs.getUri().toString(), "/" + t.getTableName() + "_"+System.currentTimeMillis()
				+ "_tmp");
		if (fs.exists(tempDir)){
			throw new IOException("Dir "+tempDir.toString()+" is existed!");
		}

		fs.mkdirs(tempDir);
		

		isInsert = false;
		method = "addIndex " + getReduceNum(indexTableName.size()+1);
		src = null;

		// init the load class
		load = new LoadHFiles(config);
	}

	/**
	 * Used for importing data and index(optional) into table
	 * 
	 * @param tableName
	 *            the table name of which to import data
	 * @param dataFormat
	 *            the data format of import data
	 * @param srcDir
	 *            the directory of the original data in HDFS
	 * @param separator
	 *            the separator in the data that split the data in different
	 *            column
	 * @param type
	 *            describe the different type of the data, include "u","iu","i"
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public BulkLoad(String tableName,
			Map<String, ArrayList<String>> dataFormat, String srcDir,
			byte separator, String type) throws IOException,
			InterruptedException {
		this(new TableInfo(tableName, dataFormat, separator), srcDir, type);
	}

	/**
	 * Used for importing data and index(optional) into table
	 * 
	 * @param info
	 *            data format string, it contains table name, separator, column
	 *            info and index, eg.
	 *            test,test_CCT,9,f:1:String:CCINDEX:test_f_1_CCIT
	 * @param srcDir
	 *            the directory of the original data in HDFS
	 * @param type
	 *            describe the different type of the data, include "u","iu","i"
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public BulkLoad(String info, String srcDir, String type)
			throws IOException, InterruptedException {
		this(new TableInfo(info), srcDir, type);
	}
	
	public BulkLoad(TableInfo tab, String srcDir, String type) throws IOException, InterruptedException{
		this(HBaseConfiguration.create(),tab,srcDir,type);
	}

	/**
	 * Used for importing data and index(optional) into table
	 * 
	 * @param tab
	 *            the table info, it contains table name, separator, column info
	 *            and index etc.
	 * @param srcDir
	 *            the directory of the original data in HDFS
	 * @param type
	 *            describe the different type of the data, include "u","iu","i"
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public BulkLoad(Configuration conf, TableInfo tab, String srcDir, String type)
			throws IOException, InterruptedException {
		startTime = System.currentTimeMillis();

		config = conf;
		//**********************************qihouliang*****************/
		config.set("fs.default.name","data12:9000");
		//**********************************qihouliang*****************/
		fs = FileSystem.get(config);
		admin = new HBaseAdmin(config);
		
		//stop balance table
    oldBalanceValue = admin.setBalancerRunning(false, false);
    // admin.setRSAutoSplit(false);

		// init the load class
		load = new LoadHFiles(config);

		tab.addIndexInfo(config);
		t = tab;
		indexTableName = getAllIndexTableName(t);
		
//////////////////////////////qihouliang-test/////////////////////////////
		System.out.println("indexTableName.toString()--------->"+indexTableName.toString());
//////////////////////////////qihouliang-test/////////////////////////////

		// write all index table information to config
		t.writeIndexToConfig(config);

		// create the output dir
		tempDir = new Path(fs.getUri().toString(), "/" + t.getTableName()+"_"+System.currentTimeMillis()
				+ "_tmp");
		
//////////////////////////////qihouliang-test/////////////////////////////
System.out.println("tempDir--------->"+tempDir.toString());
//////////////////////////////qihouliang-test/////////////////////////////
		if (fs.exists(tempDir))
			throw new IOException("Dir "+tempDir.toString()+" is existed!");
		
		fs.mkdirs(tempDir);

		// get the absolute path of the data dir
		FileStatus srcStatus = fs.getFileStatus(new Path(srcDir));
		src = srcStatus.getPath().toString();

//////////////////////////////qihouliang-test/////////////////////////////
//System.out.println("src--------->"+src);
//		System.out.println("t.getTableName()--------->"+t.getTableName());
//////////////////////////////qihouliang-test/////////////////////////////
		
		// Whether the table has data
		HTable table = new HTable(config, t.getTableName());
		ResultScanner scanner = table.getScanner(new Scan());
		if (scanner.next() == null)
			isInsert = false;
		else
			isInsert = true;

		scanner.close();

		//get reducer number
		int reduceNumber = 0;
		if(isInsert){
			config.set("mapreduce.totalorderpartitioner.path",
					tempDir.toString() + "/" + t.getTableName()
							+ "_partition.lst");
			List<ImmutableBytesWritable> startKeys = getAllStartKeys(indexTableName);

			HFileOutput.writePartitions(config, startKeys);
			reduceNumber = startKeys.size();
		} else {
			reduceNumber = getReduceNum(indexTableName.size());
//////////////////////////////qihouliang-test/////////////////////////////
//System.out.println("reduceNumber()--------->"+reduceNumber);
//System.out.println("indexTableName.size()--------->"+indexTableName.size());
//////////////////////////////qihouliang-test/////////////////////////////
		}
		
		// deal with different "type" case
		if (type.startsWith("iu")) {
			method = "iu";
		} else if (type.startsWith("u")) {
			method = "u "+reduceNumber;
		} else {
			if (isInsert) {
				HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(t
						.getTableName()));
				String rowkeySplit = htd.getValue("incremental.rowkey.split");
				int firstLen = Integer.parseInt(rowkeySplit.split(":", 2)[0]);
				byte[] lastRowkey = getLastRowkey(table);
				int maxID = Integer.valueOf(Bytes.toString(lastRowkey)
						.substring(0, firstLen));
				method = String.format("i %s:%d", rowkeySplit, maxID + 1);
//				System.out.println(rowkeySplit + "  MaxID : " + maxID);
			} else {

				int secondLen = countLength(src);
				int keyLen = Integer.parseInt(type.split(" ", 2)[1]);
				int requireLen = isSuitable(src, keyLen);
//////////////////////////////qihouliang-test/////////////////////////////
//System.out.println("src--------->"+src);
//System.out.println("secondLen--------->"+secondLen);
//////////////////////////////qihouliang-test/////////////////////////////
				if(requireLen != 0){
					throw new IOException("Rowkey length is too short for import, required length is "+requireLen);
				}
				method = String.format("i %d:%d:0", keyLen - secondLen,
						secondLen);
				HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(t
						.getTableName()));
				htd.setValue("incremental.rowkey.split", keyLen - secondLen
						+ ":" + secondLen);
				if (!admin.isTableDisabled(t.getTableName()))
					admin.disableTable(t.getTableName());
				admin.modifyTable(Bytes.toBytes(t.getTableName()), htd);
				admin.enableTable(t.getTableName());
			}
			
			if(t.getIndexPos().length != 0)
				method += " "+reduceNumber;
		}

		table.close();

	}

	/**
	 * Get all the start keys of related table, include index table
	 * 
	 * @param indexTableNames
	 *            all the table names
	 * @return all the start keys
	 * @throws IOException
	 */
	private List<ImmutableBytesWritable> getAllStartKeys(
			ArrayList<String> indexTableNames) throws IOException {
		List<ImmutableBytesWritable> startKeys;
		if (indexTableNames.size() != 1) {
			startKeys = new ArrayList<ImmutableBytesWritable>();
			int count = 0;
			for (String tab : indexTableNames) {
				List<ImmutableBytesWritable> tempKey = HFileOutput
						.getRegionStartKeys(new HTable(config, tab));
				TreeSet<ImmutableBytesWritable> keys = new TreeSet<ImmutableBytesWritable>(
						tempKey);
				for (ImmutableBytesWritable ibw : keys) {
					byte[] ibwBytes = new byte[ibw.getLength() + 1];
					Bytes.putByte(ibwBytes, 0, (byte) count);
					Bytes.putBytes(ibwBytes, 1, ibw.get(), ibw.getOffset(),
							ibw.getLength());
					ibw.set(ibwBytes);
				}
				count++;
				startKeys.addAll(keys);
			}
		} else {
			startKeys = HFileOutput.getRegionStartKeys(new HTable(config, t
					.getTableName()));
		}
		return startKeys;
	}

	/**
	 * Get the reduce task number according to the number of CPU cores
	 * @param i 
	 * 
	 * @return the reduce task number
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private int getReduceNum(int min) throws IOException, InterruptedException {
		int num = 0;
		// Process process = Runtime.getRuntime().exec(
		// "cat " + hadoopHome + "/conf/slaves");
		// process.waitFor();
		URL slaveURL = BulkLoad.class.getClassLoader().getResource("slaves");
		if (slaveURL == null)
			throw new IOException(
					"Could not found slaves in CONF_DIR. Please check your configurations.");

		BufferedReader read = new BufferedReader(new InputStreamReader(
				new FileInputStream(new File(slaveURL.getPath()))));
		while (read.readLine() != null)
			num++;
		read.close();
//////////////////////////////qihouliang-test/////////////////////////////
//System.out.println("num--------->"+num);
//////////////////////////////qihouliang-test/////////////////////////////
		int calNum = (int) (num	* config.getInt("mapred.tasktracker.reduce.tasks.maximum", 2) * 1.85);
		if(calNum < min)
			return min;
		else
			return calNum;
	}

	/**
	 * Get the table name and the related index table name
	 * 
	 * @param table
	 *            the table info
	 * @return the table name and the related index table name
	 */
	private ArrayList<String> getAllIndexTableName(TableInfo table) {
		int[] indexPos = table.getIndexPos();
		ArrayList<String> indexTable = new ArrayList<String>();
		indexTable.add(table.getTableName());
		for (int p : indexPos) {
			TableInfo.ColumnInfo ci = table.getColumnInfo(p);
			indexTable.add(ci.getIndexTableName());
		}
		return indexTable;
	}

	/**
	 * Get the new index table name
	 * 
	 * @param table
	 *            the table info
	 * @return the new index table name
	 */
	private ArrayList<String> getAllAddIndexTableName(TableInfo table) {
		int[] indexPos = table.getIndexPos();
		ArrayList<String> indexTable = new ArrayList<String>();

		for (int p : indexPos) {
			TableInfo.ColumnInfo ci = table.getColumnInfo(p);
			indexTable.add(ci.getIndexTableName());
		}
		return indexTable;
	}

	/**
	 * Estimate the number of records in a single split, and return the length
	 * of the number
	 * 
	 * @param source
	 *            the data dir
	 * @return the length of the number
	 */
	private static int countLength(String source) {
		long avgLen = getAvgLen(source);
		long blockSize = getBlockSize(source);
		long num = blockSize / avgLen + 1;
////////////////////////////////////qihouliang-test////////////////////////////
//System.out.println("num------------>"+num);
//System.out.println("Long.toString(num).length()------------>"+Long.toString(num).length());
////////////////////////////////////qihouliang-test////////////////////////////
		return Long.toString(num).length();

	}

	/**
	 * Get the block size
	 * 
	 * @param source
	 *            the data dir
	 * @return the block size
	 */
	private static long getBlockSize(String source) {
		try {
			FileSystem f = FileSystem.get(new Path(source).toUri(),
					new Configuration());
			List<Path> files = BulkLoadUtil.scanDirectory(f, new Path(source));
			return f.getFileStatus(files.get(0)).getBlockSize();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

	/**
	 * Get the average length of a single line in the data files
	 * 
	 * @param source
	 *            the data dir
	 * @return the average length
	 */
	private static long getAvgLen(String source) {
		long size = 0;
		long line = 0;

		try {

			FileSystem f = FileSystem.get(new Path(source).toUri(),
					new Configuration());
			List<Path> files = BulkLoadUtil.scanDirectory(f, new Path(source));

			FSDataInputStream hdfsInStream = f.open(files.get(0));
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					hdfsInStream));
			String l;
			while ((l = reader.readLine()) != null && line <= 10000) {
////////////////////////////////////qihouliang-test////////////////////////////
//System.out.println("l------------------------->"+l);
////////////////////////////////////qihouliang-test////////////////////////////
				line++;
				size += l.length();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
////////////////////////////////////qihouliang-test////////////////////////////
//System.out.println("size / line------------------------->"+size / line);
////////////////////////////////////qihouliang-test////////////////////////////
		return size / line;
	}

	// /////////////////////////////////////////////////////////
	// Incremental Rowkey
	// /////////////////////////////////////////////////////////

	/**
	 * Get the last rowkey of the table
	 * 
	 * @param table
	 *            which table to scan
	 * @return the last rowkey
	 * @throws IOException
	 */
	private byte[] getLastRowkey(HTable table) throws IOException {
		byte[] lastRowkey = HConstants.EMPTY_BYTE_ARRAY;

		Map<byte[], HRegionInfo> region = new TreeMap<byte[], HRegionInfo>(
				Bytes.BYTES_COMPARATOR).descendingMap();

    Set<HRegionInfo> regionsInfo = table.getRegionLocations().keySet();
		for (HRegionInfo info : regionsInfo) {
			region.put(info.getStartKey(), info);
		}

		HRegionInfo lastRegion = null;
		Iterator<Entry<byte[], HRegionInfo>> iter = region.entrySet()
				.iterator();
		while (iter.hasNext()
				&& Bytes.compareTo(lastRowkey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
			Map.Entry<byte[], HRegionInfo> entry = iter.next();
			lastRegion = entry.getValue();
			Path regionDir = new Path(config.get("hbase.rootdir") + "/"
					+ t.getTableName(), lastRegion.getEncodedName());
			List<Path> regionFiles = getStoreFiles(fs, regionDir);
			for (Path file : regionFiles) {
//				System.out.println(file.toString());
				FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs,file);
				long fileSize = fs.getFileStatus(file).getLen();
				FixedFileTrailer trailer = 
						FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
        HFile.Reader reader = HFile.createReader(fs, file, new CacheConfig(config), config);
            //new HFileReaderV3(file, trailer, fsdis, lines, new CacheConfig(config), new HFileSystem(fs), config);
       
				
        //HFile.Reader reader =
            //new HFileReaderV2(file, null, null, lines, new CacheConfig(config), new HFileSystem(fs), config);
				reader.loadFileInfo();
				if (Bytes.compareTo(reader.getLastRowKey(), lastRowkey) > 0)
					lastRowkey = reader.getLastRowKey();
				reader.close();
			}
		}

		return lastRowkey;
	}

	/**
	 * Get all storefiles that already exists
	 * 
	 * @param fs
	 *            the HDFS filesystem
	 * @param regionDir
	 *            the region dir to scan
	 * @return all the storefiles of the region dir
	 * @throws IOException
	 */
	public static List<Path> getStoreFiles(FileSystem fs, Path regionDir)
			throws IOException {
		List<Path> res = new ArrayList<Path>();
		PathFilter dirFilter = new FSUtils.DirFilter(fs);
		FileStatus[] familyDirs = fs.listStatus(regionDir, dirFilter);
		for (FileStatus dir : familyDirs) {
			if (dir.getPath().getName().matches("(_|\\.).*"))
				continue;
			FileStatus[] files = fs.listStatus(dir.getPath());
			for (FileStatus file : files) {
				if (!file.isDir()) {
					res.add(file.getPath());
				}
			}
		}
		return res;
	}

	/**
	 * In incremental rowkey, needs move last region and create new region
	 * 
	 * @param table
	 *            which table to scan
	 * @throws IOException
	 */
	private void moveLastRegion(HTable table) throws IOException {
//		Path tempDir = new Path(fs.getUri().toString(), "/" + t.getTableName()
//				+ "_tmp");
		Path outputDir = new Path(tempDir, "output/" + t.getTableName());

		// Find last region
		TreeMap<byte[], HRegionInfo> region = new TreeMap<byte[], HRegionInfo>(
				Bytes.BYTES_COMPARATOR);
    Set<HRegionInfo> regionsInfo = table.getRegionLocations().keySet();
		for (HRegionInfo info : regionsInfo) {
			region.put(info.getStartKey(), info);
		}

		HRegionInfo lastRegion = region.get(region.lastKey());
		// Move last region data to temp dir and write meta
		Path regionDir = new Path(config.get("hbase.rootdir") + "/"
				+ t.getTableName(), lastRegion.getEncodedName());
		List<Path> regionFiles = getStoreFiles(fs, regionDir);
		if (!regionFiles.isEmpty()) {
			for (Path file : regionFiles) {
				fs.rename(file, new Path(outputDir, file.getParent().getName()
						+ "/" + file.getName()));
			}

		}
		// remove last region
		HTable meta = new HTable(config, HConstants.META_TABLE_NAME);
		ResultScanner s = meta.getScanner(new Scan());
		for (Result result : s) {
			HRegionInfo info =
			           HRegionInfo.parseFromOrNull(result.getValue(HConstants.CATALOG_FAMILY,
			        		   HConstants.REGIONINFO_QUALIFIER));
			if (info.equals(lastRegion)) {
				meta.delete(new Delete(result.getRow()));
			}
		}
		meta.close();

		fs.delete(regionDir, true);

		HConnection conn = table.getConnection();
		conn.clearRegionCache();
	}

	/**
	 * Create new region depends on split
	 * 
	 * @param table
	 *            where is the region on
	 * @param split
	 *            use the split to create new regions
	 * @throws IOException
	 */
	private void createNewRegion(HTable table, byte[][] split)
			throws IOException {
		HTable meta = new HTable(config, HConstants.META_TABLE_NAME);
		for (int i = 0; i < split.length; i++) {
			int j = (i + 1) % split.length;
			byte[] endkey;
			if (j == 0)
				endkey = HConstants.EMPTY_BYTE_ARRAY.clone();
			else
				endkey = split[j];

      HRegionInfo hri =
          new HRegionInfo(table.getTableDescriptor().getTableName(),
					split[i], endkey);
			Put put = new Put(hri.getRegionName());
			put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
       hri.toByteArray());
			meta.put(put);
		}
		meta.close();
	}

	// /////////////////////////////////////////////////////////
	// General
	// /////////////////////////////////////////////////////////

	@SuppressWarnings("unused")
	private int countFiles(Path tablePath) throws IOException {
		int count = 0;
		FileStatus fileList[] = fs.listStatus(tablePath);
		int fileNum = fileList.length;
		for (int fileCount = 0; fileCount < fileNum; fileCount++) {
			if (fileList[fileCount].getPath().getName().matches("(_|\\.).*"))
				continue;

			if (fileList[fileCount].isDir())
				count += countFiles(fileList[fileCount].getPath());
			else if (fileList[fileCount].getLen() != 0)
				count++;
		}
		return count;
	}

	/**
	 * According to the HFiles, create the region split
	 * 
	 * @param tableName
	 *            the table that should import data
	 * @return the map between startkeys and the HFiles
	 * @throws IOException
	 */

	private TreeMap<byte[], List<Path>> getNewRegion(String tableName)
			throws IOException {
//		Path tempDir = new Path(fs.getUri().toString(), "/" + t.getTableName()
//				+ "_tmp");
		Path outputDir = new Path(tempDir, "output");
		Path tablePath = new Path(outputDir, tableName);
		System.out.println("tablePath tosting: "+tablePath.toString());
		System.out.println("tablePath touri: "+tablePath.toUri().getPath());
		System.out.println(fs.getName()+" ^^^^^^^^^^^^^^^^^^^");
		if (!fs.exists(tablePath))
			throw new IOException(
					"HFile is not found! Please check your input arguments.");
		FileStatus[] familyDir = fs.listStatus(tablePath);

		TreeMap<byte[], List<Path>> temp = new TreeMap<byte[], List<Path>>(
				Bytes.BYTES_COMPARATOR);
		Map<String, List<Path>> hfileMap = new HashMap<String, List<Path>>();
		
		for (FileStatus family : familyDir) {
			Path[] hfiles = FileUtil
					.stat2Paths(fs.listStatus(family.getPath()));
			for (Path hfile : hfiles) {
				String fileName = hfile.getName();
				if(hfileMap.containsKey(fileName)){
					hfileMap.get(fileName).add(hfile);
				} else {
					List<Path> tempPath = new ArrayList<Path>();
					tempPath.add(hfile);
					hfileMap.put(fileName, tempPath);
				}
				
			}
		}
		
		for(Map.Entry<String, List<Path>> entry : hfileMap.entrySet()){
			List<Path> list = entry.getValue();
			byte[] min = null;
			for(Path f : list){
				// open HFile and get the startrow
        // HFile.Reader hfr = new HFile.Reader(fs, f, null, false);
				
				FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs,f);
				long fileSize = fs.getFileStatus(f).getLen();
				FixedFileTrailer trailer = 
						FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
        HFile.Reader hfr = HFile.createReader(fs, f, new CacheConfig(config), config);
        
            //new HFileReaderV3(f, trailer, fsdis, lines, new CacheConfig(config), new HFileSystem(fs), config);
        	
				final byte[] first;
				try {
					hfr.loadFileInfo();
					first = hfr.getFirstRowKey();
				} finally {
					hfr.close();
				}
				if(min == null || Bytes.BYTES_COMPARATOR.compare(min, first)>0){
					min = first;
				}
			}
			temp.put(min, list);
		}

		final long maxsize = config.getLong("hbase.hregion.max.filesize",
				268435456);

		byte[] startKey = null;
		long totalSize = 0;
		List<Path> pathList = new ArrayList<Path>();
		TreeMap<byte[], List<Path>> region = new TreeMap<byte[], List<Path>>(
				Bytes.BYTES_COMPARATOR);

		for (Map.Entry<byte[], List<Path>> entry : temp.entrySet()) {
			if (startKey == null)
				startKey = entry.getKey();

			long filesSize = 0;
			List<Path> tempList = new ArrayList<Path>();
			List<Path> strings = entry.getValue();
			for (Path path : strings) {
				FileStatus[] f = fs.listStatus(path);
				filesSize += f[0].getLen();
				tempList.add(f[0].getPath());
			}

			if (totalSize + filesSize > maxsize) {
				totalSize = filesSize;
				region.put(startKey, pathList);
				startKey = entry.getKey();
				pathList = new ArrayList<Path>();
				pathList.addAll(tempList);
			} else {
				totalSize += filesSize;
				pathList.addAll(tempList);
			}
		}
		region.put(startKey, pathList);
		
		int i=0;
		for(byte[] key : region.keySet()){
			LOG.debug("region "+i+": "+Bytes.toString(key));
			i++;
		}

		return region;

	}

	/**
	 * Create new regions depend on the startkeys
	 * 
	 * @param table
	 *            the table name of which needs to create new region
	 * @param startKeys
	 *            the startkeys of the new regions
	 * @throws IOException
	 */
	private void createMultiRegions(HTable table, byte[][] startKeys)
			throws IOException {
		Arrays.sort(startKeys, Bytes.BYTES_COMPARATOR);
		HTable meta = new HTable(config, HConstants.META_TABLE_NAME);

		// remove empty region - this is tricky as the mini cluster during the
		// test
		// setup already has the "<tablename>,,123456789" row with an empty
		// start
		// and end key. Adding the custom regions below adds those blindly,
		// including the new start region from empty to "bbb". lg
		List<byte[]> rows = getMetaTableRows(table.getTableName());
		List<HRegionInfo> newRegions = new ArrayList<HRegionInfo>(
				startKeys.length);
		// add custom ones
		for (int i = 0; i < startKeys.length; i++) {
			int j = (i + 1) % startKeys.length;
      HRegionInfo hri =
          new HRegionInfo(table.getTableDescriptor().getTableName(),
					startKeys[i], startKeys[j]);
			Put put = new Put(hri.getRegionName());
			put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        hri.toByteArray());
			meta.put(put);
			newRegions.add(hri);
		}
		// see comment above, remove "old" (or previous) single region
		for (byte[] row : rows) {
			meta.delete(new Delete(row));
			
		}
		// flush cache of regions
		HConnection conn = table.getConnection();
		conn.clearRegionCache();
	}

	/**
	 * Get the meta rows of the table
	 * 
	 * @param tableName
	 * @return the meta rows of the table
	 * @throws IOException
	 */
	private List<byte[]> getMetaTableRows(byte[] tableName) throws IOException {
		HTable t = new HTable(config, HConstants.META_TABLE_NAME);
		List<byte[]> rows = new ArrayList<byte[]>();
		ResultScanner s = t.getScanner(new Scan());
		for (Result result : s) {
			if(result == null)
				continue;
      HRegionInfo info =
           HRegionInfo.parseFromOrNull(result.getValue(HConstants.CATALOG_FAMILY,
        		   HConstants.REGIONINFO_QUALIFIER));
       //HTableDescriptor desc = info.getTableDesc();
      if (Bytes.compareTo(info.getTableName(), tableName) == 0) {

				rows.add(result.getRow());
			}
		}
		s.close();
		return rows;
	}

	/**
	 * According to the class name, get jar file path that contains that class
	 * 
	 * @param clazz
	 * @return
	 */
	// private static String getJarPath(Class<?> clazz) {
	// String path = clazz.getProtectionDomain().getCodeSource().getLocation()
	// .getFile();
	// try {
	// path = java.net.URLDecoder.decode(path, "UTF-8");
	// } catch (java.io.UnsupportedEncodingException e) {
	// e.printStackTrace();
	// }
	//
	// java.io.File jarFile = new java.io.File(path);
	// return jarFile.getAbsolutePath();
	// }

	/**
	 * Check whether the input arguments are correct or not
	 * 
	 * @throws IOException
	 */
	public void preTest() throws IOException {
		// Get some data of the input files
		List<Path> fList = BulkLoadUtil.scanDirectory(fs, new Path(src));
		if (fList.size() == 0) {
			throw new IOException("The data directory is not correct!");
		}

		Path[] filePath = fList.toArray(new Path[0]);
		int line = 0;
		int errorLines = 0;
		int columnNum = t.getColumnInfo().size();
		for (int i = 0; i < filePath.length && line < 10000; i++) {
			FSDataInputStream hdfsInStream = fs.open(filePath[i]);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					hdfsInStream));
			try {
				String l;
				while ((l = reader.readLine()) != null && line < 10000) {
					line++;
					if (method.startsWith("u") || method.startsWith("iu")) {
						Integer[] split = BulkLoadUtil.dataSplit(
								t.getSeparator(), Bytes.toBytes(l),
								Bytes.toBytes(l).length);
						if (split.length != columnNum + 2)
							errorLines++;
					} else {
						Integer[] split = BulkLoadUtil.dataSplit(
								t.getSeparator(), Bytes.toBytes(l),
								Bytes.toBytes(l).length);
						if (split.length != columnNum + 1)
							errorLines++;
					}
				}
			} finally {
				reader.close();
			}
		}
		if (((double) errorLines / line) > 0.8) {
			throw new IOException(
					"The data format or the separator is not correct!");
		}

		// following do not support data with index and insert data, because
		// tests need muti reducer
		if (t.getIndexPos().length > 0 || isInsert == true)
			return;

		// check dir
		FileSystem localFS = FileSystem.getLocal(new Configuration());
		if (localFS.exists(new Path(PRE_TEST_DIR)))
			localFS.delete(new Path(PRE_TEST_DIR), true);

		// create a test data file
		BufferedReader hdfsReader = new BufferedReader(new InputStreamReader(
				fs.open(filePath[0])));
		BufferedWriter fileWriter = new BufferedWriter(new OutputStreamWriter(
				localFS.create(new Path(PRE_TEST_DIR + "/data/file"))));
		try {
			String str;
			int count = 0;
			while ((str = hdfsReader.readLine()) != null && count < 10000) {
				count++;
				fileWriter.write(str + "\n");
			}
		} finally {
			fileWriter.close();
			hdfsReader.close();
		}

		if (!MRImport.runPreTest(method, PRE_TEST_DIR + "/data", isInsert,
				config)) {
			throw new IOException(
					"Job failed! Please check your input arguments.");
		}
	}

	/**
	 * Run the MapReduce job and load HFiles
	 * 
	 * @throws IOException
	 */
	public void run() throws IOException {

		try {
			im = new MRImport(method, src, isInsert, config, tempDir);
		} catch (IOException e) {
			LOG.error("Invalid agruments: ", e);
			status = "Invalid agruments: " + e;
			success = false;
			complete = true;
			return;
		}
		new Thread() {
			public void run() {

				status = "In processing";
				try {

					im.run();
				} catch (Exception e) {
					LOG.error(
							"Job Failed! Please check JobTracker page for more detail.",
							e);
					status = "Job Failed! Please check JobTracker page for more detail."
							+ e;
					success = false;
					complete = true;
					return;
				}

				lines = im.getLinesNum();
				size = im.getInputSize();
				outSize = im.getOutputSize();

//				Path tempDir = new Path(fs.getUri().toString(), "/"
//						+ t.getTableName() + "_tmp");

				Path outputDir = new Path(tempDir, "output");

				status = "Start load HFiles";
				// load HFiles
				try {
					if (isInsert) {
						if (method.startsWith("u")) {
							for (String indexTable : indexTableName) {
								if (admin.isTableDisabled(indexTable))
									admin.enableTable(indexTable);
								while (!admin.isTableAvailable(indexTable))
									Thread.sleep(1000);
							}
							load.doBulkLoad(outputDir,
									indexTableName.toArray(new String[0]));

						} else {

							final HTable table = new HTable(config,
									t.getTableName());
							admin.disableTable(t.getTableName());
							moveLastRegion(table);
							System.out.println("get new region: "+t.getTableName());
							TreeMap<byte[], List<Path>> regionMap = getNewRegion(t
									.getTableName());
							byte[][] split = regionMap.keySet().toArray(
									new byte[0][]);
							createNewRegion(table, split);
							admin.enableTable(t.getTableName());

							load.doBulkLoad(outputDir,
									new String[] { t.getTableName() });
						}

					} else {
						
						Map<String, Map<byte[], List<Path>>> loadMap = new HashMap<String, Map<byte[],List<Path>>>();
						for (String indexTable : indexTableName) {
							System.out.println("indexTable-------------->"+indexTable+"...----------->qihouliang");
							TreeMap<byte[], List<Path>> regionMap = getNewRegion(indexTable);
							byte[][] split = regionMap.keySet().toArray(
									new byte[0][]);

							byte[][] startKeys = new byte[split.length + 1][];
							startKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
							System.arraycopy(split, 0, startKeys, 1,
									split.length);
							
							HTable table = new HTable(config, indexTable);
							admin.disableTable(table.getTableName());
							createMultiRegions(table, startKeys);
							admin.enableTableAsync(table.getTableName());
							while (!admin.isTableEnabled(table.getTableName())) {
								Thread.sleep(1000);
							}
							loadMap.put(indexTable, regionMap);
						}
						// load all table hfiles into hbase
						load.doQuickBulkLoad(loadMap);

					}
					cleanup();
				} catch (Exception e) {
					LOG.error("Load HFile failed: ", e);
					status = "Load HFile failed: " + e.toString();
					success = false;
					complete = true;
					return;
				}
				long time = (System.currentTimeMillis() - startTime) / 1000;
				result = String.format("Total Time: %ds\nImport Lines:%d\nImport Size:%d\nWriten Size:%d\nThroughput(import size/time):%fMB/s\nThroughput(writen size/time):%fMB/s",
								time, lines, size, outSize, (double) size / 1024 / 1024 / time, 
								(double) outSize / 1024 / 1024 / time);
				
				success = true;
				complete = true;

			}

		}.start();
	}

	/**
	 * Turn on balance and delete output directory
	 * @throws IOException
	 */
	private void cleanup() throws IOException {
    // admin.balanceSwitch(oldBalanceValue);
    admin.setBalancerRunning(false, false);
    // admin.setRSAutoSplit(true);
    // UNDEFINED
//		Path tempDir = new Path(fs.getUri().toString(), "/"
//				+ t.getTableName() + "_tmp");
		fs.delete(tempDir, true);
		if(method.startsWith("addIndex")){
			for(String tableName : this.indexTableName){
				admin.disableTable(tableName);
			}
			admin.disableTable(t.getTableName());
		}
	}

	/**
	 * Get the program progress
	 * 
	 * @return the progress
	 */
	public int getProgress() {
		return (int) (100 * (0.85 * im.getProgress() + 0.15 * load.getProgress()));
	}

	/**
	 * Whether the MapReduce job is finished
	 * 
	 * @return true if the MapReduce job is finished
	 */
	public boolean isComplete() {
		return complete;
	}

	/**
	 * Whether the MapReduce job is successful
	 * 
	 * @return true if the MapReduce job is successful
	 */
	public boolean isSuccessful() {
		return success;
	}

	/**
	 * Get some result report
	 * 
	 * @return the result include data size and the number of lines
	 */
	public String getResult() {
		return result;
	}

	/**
	 * Get running status
	 * 
	 * @return the status of the running program
	 */
	public String getStatus() {
		return status;
	}

	/**
	 * Whether the given length of rowkey is suit the current situation
	 * 
	 * @param srcDir
	 *            the data dir
	 * @param length
	 *            the given length
	 * @return
	 */
	public static int isSuitable(String srcDir, int length) {
		int secondLen = countLength(srcDir);
		int firstLen = getSplitSize(srcDir);
		return firstLen + secondLen > length ? firstLen + secondLen : 0;
	}

	/**
	 * Get the size of a single split
	 * 
	 * @param srcDir
	 *            the data dir
	 * @return
	 */
	private static int getSplitSize(String srcDir) {
		int split = 0;
		try {
			long blockSize = getBlockSize(srcDir);
			FileSystem f = FileSystem.get(new Path(srcDir).toUri(),
					new Configuration());
			FileStatus[] files = f.listStatus(new Path(srcDir));
			for (FileStatus file : files) {
				split += (int) Math.ceil((double)file.getLen() / blockSize);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return Integer.toString(split).length();
	}
	
	public void printRegionMap(String tableName) throws IOException {
		TreeMap<byte[], List<Path>> regionMap = getNewRegion(tableName);
		for(byte[] key : regionMap.keySet()){
			System.out.println("region :"+Bytes.toString(key));
			System.out.println("files :"+regionMap.get(key));
		}
	}

	  /**
   * In case you failed loading, use this try to load HFiles again.
   * @param tableName the table you want to load HFiles
   * @throws Exception
   */
  public void trytoLoadHFile(String tableName, Path tempDir) throws Exception {
		this.tempDir = tempDir;
		TreeMap<byte[], List<Path>> regionMap = getNewRegion(tableName);
		byte[][] split = regionMap.keySet().toArray(new byte[0][]);

		byte[][] startKeys = new byte[split.length + 1][];
		startKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
		System.arraycopy(split, 0, startKeys, 1, split.length);

		HTable table = new HTable(config, tableName);
		admin.disableTable(table.getTableName());
		createMultiRegions(table, startKeys);
		admin.enableTableAsync(table.getTableName());
		while (!admin.isTableEnabled(table.getTableName())) {
			Thread.sleep(1000);
		}
		Path outputDir = new Path(fs.getUri().toString(), "/" + tableName
				+ "_tmp/output/" + tableName);
		new LoadIncrementalHFiles(config).doBulkLoad(outputDir, table);
	}

	/**
	 * 
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * @throws NoNeedBuildIndexException 
	 */
	public static void main(String args[]) throws IOException,
			InterruptedException, ClassNotFoundException, NoNeedBuildIndexException {
		
		boolean isAddIndex = false;
		String type = "uo";
		String dir = null;
		String dataFormat = null;
		boolean preTest = false;

		Options options = new Options();
		options.addOption("s", "source", true, "Directories to import.");

		OptionGroup group = new OptionGroup();

		group.addOption(new Option(
				"l",
				"rowkeylen",
				true,
				"Program will generate incremental rowkey. You must specify the length of the rowkey."));
		group.addOption(new Option("uo", "unorder", false,
				"Rowkey is defined by user and has no order."));
		group.addOption(new Option("o", "order", false,
				"Rowkey is defined by user and should be ordered."));
		group.addOption(new Option("addIndex", "Add Index to exist table"));
		options.addOptionGroup(group);

		OptionGroup dfGroup = new OptionGroup();

		dfGroup.addOption(new Option(
				"ts",
				"tablestruct",
				true,
				"Describe the format of data, it's described as: tablename,comma|tab|semicolon|space,segment1[,segment2,...,segmentn]."));
		dfGroup.addOption(new Option("xml", true,
				"Describe the format of data by a XML file."));
		options.addOptionGroup(dfGroup);
		// options.addOption("df", true, "The data format of origenal data.");

		options.addOption("preTest", false,
				"To start a pre-test before the program start.");

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

			if (cmd.hasOption("addIndex"))
				isAddIndex = true;

			if (cmd.hasOption("uo"))
				type = "u";

			if (cmd.hasOption("o"))
				type = "iu";

			if (cmd.hasOption("l"))
				type = "i " + cmd.getOptionValue("l");

			if (cmd.hasOption("s"))
				dir = cmd.getOptionValue("s");

			if (cmd.hasOption("ts"))
				dataFormat = cmd.getOptionValue("ts");

			if (cmd.hasOption("preTest"))
				preTest = true;

			if (cmd.hasOption("h")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("BulkLoad", options, true);
				System.exit(0);
			}
		} catch (ParseException e) {
			e.printStackTrace();
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("BulkLoad", options, true);
			System.exit(-1);
		}

		// Get table info and add index into it
		TableInfo tab = new TableInfo(dataFormat);
		tab.addIndexInfo(HBaseConfiguration.create());

		BulkLoad bl = null;
		if (isAddIndex) {

			if (tab.getIndexPos().length == 0)
				throw new IOException("No index found!");

			bl = new BulkLoad(tab);
		} else {

			bl = new BulkLoad(tab, dir, type);
			if (preTest) {
				try {
					bl.preTest();
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(-1);
				}

			}
		}

		bl.run();

		while (!bl.isComplete()) {
			System.out.println(bl.getProgress());
			Thread.sleep(3000);
		}
		System.out.println(bl.getProgress());
		if (bl.isSuccessful())
			System.out.println(bl.getResult());
	}
}
