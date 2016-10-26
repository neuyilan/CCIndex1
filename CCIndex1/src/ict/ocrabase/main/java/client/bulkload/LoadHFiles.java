/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ict.ocrabase.main.java.client.bulkload;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionServerCallable;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileReaderV3;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.ServiceException;

/**
 * Tool to load the output of HFileOutputFormat into an existing table.
 * @see #usage()
 */

public class LoadHFiles extends Configured {

  static Log LOG = LogFactory.getLog(LoadHFiles.class);

  public static String NAME = "completebulkload";
  
//  private volatile int total;
//  private volatile int count;
  private volatile float progress;

  public LoadHFiles(Configuration conf) {
    super(conf);
  }

  public LoadHFiles() {
    super();
  }

  /**
   * Represents an HFile waiting to be loaded. An queue is used
   * in this class in order to support the case where a region has
   * split during the process of the load. When this happens,
   * the HFile is split into two physical parts across the new
   * region boundary, and each part is added back into the queue.
   * The import process finishes when the queue is empty.
   */
  private static class LoadQueueItem {
    final byte[] family;
    final Path hfilePath;

    public LoadQueueItem(byte[] family, Path hfilePath) {
      this.family = family;
      this.hfilePath = hfilePath;
    }
  }

  /**
   * Walk the given directory for all HFiles, and return a Queue
   * containing all such files.
   */
  private Deque<LoadQueueItem> discoverLoadQueue(Path hfofDir)
  throws IOException {
    FileSystem fs = hfofDir.getFileSystem(getConf());

    if (!fs.exists(hfofDir)) {
      throw new FileNotFoundException("HFileOutputFormat dir " +
          hfofDir + " not found");
    }

    FileStatus[] familyDirStatuses = fs.listStatus(hfofDir);
    if (familyDirStatuses == null) {
      throw new FileNotFoundException("No families found in " + hfofDir);
    }

    Deque<LoadQueueItem> ret = new LinkedList<LoadQueueItem>();
    for (FileStatus stat : familyDirStatuses) {
      if (!stat.isDir()) {
        LOG.warn("Skipping non-directory " + stat.getPath());
        continue;
      }
      Path familyDir = stat.getPath();
      // Skip _logs, etc
      if (familyDir.getName().startsWith("_")) continue;
      byte[] family = familyDir.getName().getBytes();
      Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(familyDir));
      for (Path hfile : hfiles) {
        if (hfile.getName().startsWith("_")) continue;
        ret.add(new LoadQueueItem(family, hfile));
      }
    }
    return ret;
  }

  public float getProgress(){
	  return progress;
  }
  
  /**
   * Perform a bulk load of the given directory into the given
   * pre-existing table.
   * @param hfofDir the directory that was provided as the output path
   * of a job using HFileOutputFormat
   * @param table the table to load into
   * @throws TableNotFoundException if table does not yet exist
   */
	public void doBulkLoad(Path outDir, String[] tableNames)
			throws TableNotFoundException, IOException
  {
	  Configuration config = getConf();
	  int num = 0;
	  int tableNum = tableNames.length;
	  for(String tableName : tableNames){
		  HTable table  = new HTable(config, tableName);
		  Path hfofDir = new Path(outDir, tableName);
		  HConnection conn = table.getConnection();

		  if (!conn.isTableAvailable(table.getTableName())) {
			  throw new TableNotFoundException("Table " +
					  Bytes.toStringBinary(table.getTableName()) +
					  "is not currently available.");
		  }

		  Deque<LoadQueueItem> queue = null;
		  try {
			  queue = discoverLoadQueue(hfofDir);
			  int total = queue.size();
			  while (!queue.isEmpty()) {
				  LoadQueueItem item = queue.remove();
				  tryLoad(item, conn, table.getTableName(), queue, config);
				  progress = (num + 1 - (float)queue.size() / total) / tableNum;
			  }
		  } finally {
			  if (queue != null && !queue.isEmpty()) {
				  StringBuilder err = new StringBuilder();
				  err.append("-------------------------------------------------\n");
				  err.append("Bulk load aborted with some files not yet loaded:\n");
				  err.append("-------------------------------------------------\n");
				  for (LoadQueueItem q : queue) {
					  err.append("  ").append(q.hfilePath).append('\n');
				  }
				  LOG.error(err);
				  throw new IOException();
			  }
		  }
		  num++;
	  }
	  progress = 1;
  }

  /**
   * Attempt to load the given load queue item into its target region server.
   * If the hfile boundary no longer fits into a region, physically splits
   * the hfile such that the new bottom half will fit, and adds the two
   * resultant hfiles back into the load queue.
   */
  private void tryLoad(final LoadQueueItem item,
      HConnection conn, final byte[] table,
      final Deque<LoadQueueItem> queue, Configuration config)
  throws IOException {
    final Path hfilePath = item.hfilePath;
   
    final FileSystem fs = hfilePath.getFileSystem(getConf());
    
    //FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs,hfilePath);
	//long fileSize = fs.getFileStatus(hfilePath).getLen();
	//FixedFileTrailer trailer = 
		//	FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
HFile.Reader hfr = HFile.createReader(fs, hfilePath, new CacheConfig(config), config);
//new HFileReaderV3(hfilePath, trailer, fsdis, 0, new CacheConfig(config), new HFileSystem(fs), config);

    
    
    //HFileReaderV3 hfr = new HFileReaderV3(hfilePath, null, 
      //null, 0, new CacheConfig(config), new HFileSystem(fs), config);
        //HFileReaderV3(fs, hfilePath, null, false);
    final byte[] first, last;
    try {
      hfr.loadFileInfo();
      first = hfr.getFirstRowKey();
      last = hfr.getLastRowKey();
      System.out.println("hfr first row key:"+first);
      System.out.println("hfr last row key:"+last);
    }  finally {
      hfr.close();
    }

    LOG.info("Trying to load hfile=" + hfilePath +
        " first=" + Bytes.toStringBinary(first) +
        " last="  + Bytes.toStringBinary(last));
    if (first == null || last == null) {
      assert first == null && last == null;
      LOG.info("hfile " + hfilePath + " has no entries, skipping");
      return;
    }

    // We use a '_' prefix which is ignored when walking directory trees
    // above.
    final Path tmpDir = new Path(item.hfilePath.getParent(), "_tmp");

    //RpcRetryingCallerFactory rpc_caller_factory =
       // RpcRetryingCallerFactory.instantiate(conn.getConfiguration(),null);

    RegionServerCallable<Void> callable =
    // conn.getRegionServerWithRetries(
        new RegionServerCallable<Void>(conn, TableName.valueOf(Bytes.toString(table)), first) {
        @Override
        public Void call() throws Exception {
          LOG.debug("Going to connect to server " + location +
              "for row " + Bytes.toStringBinary(row));
          HRegionInfo hri = location.getRegionInfo();
          if (!hri.containsRange(first, last)) {
            LOG.info("HFile at " + hfilePath + " no longer fits inside a single " +
                "region. Splitting...");

              HColumnDescriptor familyDesc =
                  this.connection.getHTableDescriptor(hri.getTable()).getFamily(item.family);
            Path botOut = new Path(tmpDir, hri.getEncodedName() + ".bottom");
            Path topOut = new Path(tmpDir, hri.getEncodedName() + ".top");
            splitStoreFile(getConf(), hfilePath, familyDesc, hri.getEndKey(),
                botOut, topOut);

            // Add these back at the *front* of the queue, so there's a lower
            // chance that the region will just split again before we get there.
            queue.addFirst(new LoadQueueItem(item.family, botOut));
            queue.addFirst(new LoadQueueItem(item.family, topOut));
            LOG.info("Successfully split into new HFiles " + botOut + " and " + topOut);
            return null;
          }

          byte[] regionName = location.getRegionInfo().getRegionName();
            // server.bulkLoadHFile(hfilePath.toString(), regionName, item.family);

            final List<Pair<byte[], String>> famPaths = new ArrayList<Pair<byte[], String>>();
            famPaths.add(Pair.newPair(item.family, hfilePath.toString()));
            BulkLoadHFileRequest request =
                RequestConverter.buildBulkLoadHFileRequest(famPaths, regionName, false);
            this.getStub().bulkLoadHFile(null, request);
          return null;
        }
        };
        Void succcess = 
					RpcRetryingCallerFactory.instantiate(
							conn.getConfiguration(),null).<Void> newCaller().callWithRetries(callable);
        System.out.println("loadHfiles- line:295");
    //rpc_caller_factory.<Void> newCaller().callWithRetries(callable);
  }

  /**
   * Split a storefile into a top and bottom half, maintaining
   * the metadata, recreating bloom filters, etc.
   */
  static void splitStoreFile(
      Configuration conf, Path inFile,
      HColumnDescriptor familyDesc, byte[] splitKey,
      Path bottomOut, Path topOut) throws IOException
  {
    // Open reader with no block cache, and not in-memory
    
    Reference topReference = Reference.createTopReference(splitKey);
    Reference bottomReference = Reference.createBottomReference(splitKey);
    

    copyHFileHalf(conf, inFile, topOut, topReference, familyDesc);
    copyHFileHalf(conf, inFile, bottomOut, bottomReference, familyDesc);
  }

  /**
   * Copy half of an HFile into a new HFile.
   */
  private static void copyHFileHalf(
      Configuration conf, Path inFile, Path outFile, Reference reference,
      HColumnDescriptor familyDescriptor)
  throws IOException {
    FileSystem fs = inFile.getFileSystem(conf);
    HalfStoreFileReader halfReader = null;
    StoreFile.Writer halfWriter = null;
    try {
      halfReader = new HalfStoreFileReader(fs, inFile, null, reference, conf);
      Map<byte[], byte[]> fileInfo = halfReader.loadFileInfo();

      int blocksize = familyDescriptor.getBlocksize();
      Algorithm compression = familyDescriptor.getCompression();
      // BloomType bloomFilterType = familyDescriptor.getBloomFilterType();
      HFileContext hFileContext =
          new HFileContextBuilder().withCompression(compression)
              .withChecksumType(HStore.getChecksumType(conf))
              .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf)).withBlockSize(blocksize)
              .withDataBlockEncoding(familyDescriptor.getDataBlockEncoding()).build();

      halfWriter =
          new StoreFile.WriterBuilder(conf, new CacheConfig(conf), fs)
              .withBloomType(familyDescriptor.getBloomFilterType())
              .withComparator(KeyValue.COMPARATOR).withOutputDir(outFile)
              .withFileContext(hFileContext).withMaxKeyCount(0).build();

      // halfWriter = new StoreFile.Writer(fs, outFile, conf,
      // new CacheConfig(conf),KeyValue.COMPARATOR,
      // familyDescriptor.getBloomFilterType(), 0, null, hFileContext);
      HFileScanner scanner = halfReader.getScanner(false, false);
      scanner.seekTo();
      do {
        KeyValue kv = scanner.getKeyValue();
        halfWriter.append(kv);
      } while (scanner.next());

      for (Map.Entry<byte[],byte[]> entry : fileInfo.entrySet()) {
        if (shouldCopyHFileMetaKey(entry.getKey())) {
          halfWriter.appendFileInfo(entry.getKey(), entry.getValue());
        }
      }
    } finally {
      if (halfWriter != null) halfWriter.close();
      if (halfReader != null) halfReader.close(false);
    }
  }

  private static boolean shouldCopyHFileMetaKey(byte[] key) {
    return !HFile.isReservedFileInfoKey(key);
  }


//  @Override
//  public int run(String[] args) throws Exception {
//    if (args.length != 2) {
//      usage();
//      return -1;
//    }
//
//    Path hfofDir = new Path(args[0]);
//    HTable table = new HTable(this.getConf(), args[1]);
//
//    doBulkLoad(hfofDir, table);
//    return 0;
//  }

//  public static void main(String[] args) throws Exception {
//    ToolRunner.run(new LoadHFiles(), args);
//  }
  
  class LoadItem {
	  public byte[] startKey;
	  public List<Path> fileList;
	  
	  public LoadItem(byte[] startKey, List<Path> fileList){
		this.startKey = startKey;
		this.fileList = fileList;
	  }
  }
  
  class LoadHFileThread extends Thread{
	  private ConcurrentLinkedQueue<LoadItem> conQueue;
	  private String tableName;
	  
	  public LoadHFileThread(String tableName, ConcurrentLinkedQueue<LoadItem> conQueue){
		  this.conQueue = conQueue;
		  this.tableName = tableName;
	  }
	  
	  public void run() {
		Configuration config = getConf();
		HTable table;
		try {
			table = new HTable(config, tableName);
		} catch (IOException e) {
			LOG.info(e);
			return ;
		}
		HConnection conn = table.getConnection();
		final Deque<LoadQueueItem> queue = new LinkedList<LoadQueueItem>();
		LoadItem item;
		
		while((item = conQueue.poll()) != null){
			
			final List<Path> list = item.fileList;
			
			
			try {
        //  RpcRetryingCallerFactory rpc_caller_factory =
          //    RpcRetryingCallerFactory.instantiate(conn.getConfiguration(),null);
          //RegionServerCallable<Void> callable =
        		 
          // conn.getRegionServerWithRetries(
        		  // new ServerCallable<Void>(conn, table.getTableName(), item.startKey) {
              
              Void succcess = 
  					RpcRetryingCallerFactory.instantiate(
  							conn.getConfiguration(),null).<Void> newCaller().callWithRetries(new RegionServerCallable<Void>(conn, table.getName(), item.startKey) {
  				               
  								@Override
  								public Void call() {
  									byte[] regionName = location.getRegionInfo().getRegionName();
  									LOG.info("Loading region "+Bytes.toString(regionName));
  									for(Path hfilePath : list){
  										byte[] family = Bytes.toBytes(hfilePath.getParent().getName());
  								        try {
  	                     
  								        	// server.bulkLoadHFile(hfilePath.toString(), regionName, family);

  	                     
  								        	final List<Pair<byte[], String>> famPaths =
  								        			new ArrayList<Pair<byte[], String>>();
  	             
  								        	famPaths.add(Pair.newPair(family, hfilePath.toString()));
  								        	
  								        	System.out.println("family-hfilepath: "+family+' '+hfilePath.toString());
  	                     
  								        	BulkLoadHFileRequest request =                      
  								        			RequestConverter.buildBulkLoadHFileRequest(famPaths, regionName, true);
  	                      
  								        	this.getStub().bulkLoadHFile(null, request);
  	                      
  								        	//return null;
  	                   
  								        } catch (ServiceException e) {
  											LOG.info(e);
  											queue.add(new LoadQueueItem(family,hfilePath));
  										}
  									}
  									return null;
  								}
  	              });
              System.out.println("loadHfiles- line:465");
			} catch (RuntimeException e) {
				LOG.info(e);
				return ;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.err.println("RPC error: Void success not success.");
				e.printStackTrace();
			}
		
			
		}
		
		try {
			System.out.println("queue: "+queue.size());
			while (!queue.isEmpty()) {
				LoadQueueItem reLoadItem = queue.remove();
          tryLoad(reLoadItem, conn, table.getTableName(), queue, config);
          

          /*
           * private void tryLoad(final LoadQueueItem item, HConnection conn, final byte[] table,
           * final Deque<LoadQueueItem> queue, Configuration config)
           */
			}
		} catch (IOException e) {
			LOG.info(e);
			return ;
		}
		  
	  }
  }

  public void doQuickBulkLoad(Map<String, Map<byte[], List<Path>>> loadMap) throws IOException, InterruptedException{
	  Configuration config = getConf();
	  int loadThreadNum = config.getInt("bulkload.loadthread.num", 10);
	  
	  for(Map.Entry<String, Map<byte[], List<Path>>> entry : loadMap.entrySet()){
		  String tableName = entry.getKey();
		  
		  LOG.info("Start loading table "+tableName);
		  
		  Map<byte[], List<Path>> regionMap = entry.getValue();
		  
		  ConcurrentLinkedQueue<LoadItem> conQueue = new ConcurrentLinkedQueue<LoadHFiles.LoadItem>();
		  for(Map.Entry<byte[], List<Path>> item : regionMap.entrySet()){
			  conQueue.add(new LoadItem(item.getKey(), item.getValue()));
		  }
		  
		  int threadNum;
		  if(regionMap.size() < loadThreadNum){
			  threadNum = regionMap.size();
		  } else {
			  threadNum = loadThreadNum;
		  }
		  LoadHFileThread[] threads = new LoadHFileThread[threadNum];
		  for(int i=0; i<threadNum; i++){
			  threads[i] = new LoadHFileThread(tableName, conQueue);
		  }
		  
		  LOG.info("Starting threads");
		  
		  for(int i=0; i<threadNum; i++){
			  threads[i].start();
		  }
		  
		  LOG.info("Started threads!");
		  
		  for(int i=0; i<threadNum; i++){
			  threads[i].join();
		  }
		  
		  progress = 1;
	  }
  }
}
