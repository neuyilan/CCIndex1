package ict.ocrabase.main.java.test;

import ict.ocrabase.main.java.client.index.IndexAdmin;
import ict.ocrabase.main.java.client.index.IndexSpecification;
import ict.ocrabase.main.java.client.index.IndexTableDescriptor;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutTest {
  private static final byte[] tableName = Bytes.toBytes("test");
//  private static final byte[] newTableName = Bytes.toBytes("NewTable");
  private static Configuration conf;
//  private static IndexAdmin indexadmin;
//  private static IndexTableDescriptor indexDesc;
//  private static HBaseAdmin admin;
//  private static IndexSpecification[] index;
//  private static HTableDescriptor desc;
//  private static HTableDescriptor newdesc;
//  private static IndexTableDescriptor newIndexDesc;

  private static HTable tempHTable;
  static long maxNum = 1000*1000*10;

  public static void main(String[] args) throws IOException {
    conf = HBaseConfiguration.create();
    
//    String attr=conf.get("hbase.coprocessor.region.classes");
//    System.out.println(attr.toString());
    
    tempHTable = new HTable(conf, tableName);
    long starttime = System.currentTimeMillis();
    Random random = new Random();
    
    for (long i = 10; i < 40; i++) {
      Put put = new Put(Bytes.toBytes(Long.toString(i)));

      put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(i));

      put.add(Bytes.toBytes("f1"), Bytes.toBytes("c2"), Bytes.toBytes(i*10));
      put.add(Bytes.toBytes("f2"), Bytes.toBytes("c3"), Bytes.toBytes(i*100));
      put.add(Bytes.toBytes("f3"), Bytes.toBytes("c4"), Bytes.toBytes(i*1000));
      
      
      tempHTable.put(put);
      

    }
    long stoptime = System.currentTimeMillis();
    System.out.println("total time consumed:  "+ Long.toString(stoptime-starttime));
    tempHTable.close();

    System.out.println("Put success!");
    return;

  }
}
