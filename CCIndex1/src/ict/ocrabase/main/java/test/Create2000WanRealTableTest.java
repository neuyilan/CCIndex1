package ict.ocrabase.main.java.test;

import ict.ocrabase.main.java.client.index.IndexAdmin;
import ict.ocrabase.main.java.client.index.IndexExistedException;
import ict.ocrabase.main.java.client.index.IndexSpecification;
import ict.ocrabase.main.java.client.index.IndexTableDescriptor;
import ict.ocrabase.main.java.client.index.IndexSpecification.IndexType;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class Create2000WanRealTableTest {
  private static final byte[] tableName = Bytes.toBytes("2000wanreal");
  private static final byte[] newTableName = Bytes.toBytes("New");
  private static Configuration conf;
  private static IndexAdmin indexadmin;
  private static IndexTableDescriptor indexDesc;
  private static HBaseAdmin admin;
  private static IndexSpecification[] index;
  private static HTableDescriptor desc;
  private static HTableDescriptor newdesc;
  private static IndexTableDescriptor newIndexDesc;

  public static void main(String[] args) throws IOException, IndexExistedException {
    conf = HBaseConfiguration.create();
//    indexadmin = new IndexAdmin(conf);
    admin = new HBaseAdmin(conf);
    // indexadmin.setTest(true);

    desc = new HTableDescriptor(TableName.valueOf(tableName));
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f2")));
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f3")));
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f4")));
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f5")));
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f6")));
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f7")));
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f8")));
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f9")));
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f10")));
//
//    
//    index = new IndexSpecification[4];
//    index[0] = new IndexSpecification(Bytes.toBytes("f1:c2"), IndexType.CCINDEX);
//    //index[3] = new IndexSpecification(Bytes.toBytes("f1:c4"), IndexType.CCINDEX);
//    index[1] = new IndexSpecification(Bytes.toBytes("f1:c1"), IndexType.SECONDARYINDEX);
//    index[2] = new IndexSpecification(Bytes.toBytes("f2:c3"), IndexType.IMPSECONDARYINDEX);
//    index[2].addAdditionColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
//    index[2].addAdditionColumn(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
//    index[3] = new IndexSpecification(Bytes.toBytes("f3:c4"), IndexType.CCINDEX);
//    //index[2].addAdditionFamily(Bytes.toBytes("f3"));
//
//    indexDesc = new IndexTableDescriptor(desc, index);
//    indexadmin.createTable(indexDesc);

    // table without index
    /*
    newdesc = new HTableDescriptor(TableName.valueOf(newTableName));
    newdesc.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
    newdesc.addFamily(new HColumnDescriptor(Bytes.toBytes("f2")));
    newIndexDesc = new IndexTableDescriptor(newdesc);
    indexadmin.createTable(newIndexDesc);
    */
    if (admin.tableExists(tableName)) {
	      admin.disableTable(tableName);
	      admin.deleteTable(tableName);
	    }
    admin.createTable(desc);
    System.out.println("Create table finished!");
  }
}
