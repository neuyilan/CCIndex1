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

public class CreateTableTest {
  private static final byte[] tableName = Bytes.toBytes("test11");
  private static Configuration conf;
  private static IndexAdmin indexadmin;
  private static IndexTableDescriptor indexDesc;
  private static HBaseAdmin admin;
  private static IndexSpecification[] index;
  private static HTableDescriptor desc;
//  private static HTableDescriptor newdesc;
//  private static IndexTableDescriptor newIndexDesc;

  public static void main(String[] args) throws IOException, IndexExistedException {
    conf = HBaseConfiguration.create();
    indexadmin = new IndexAdmin(conf);
    admin = new HBaseAdmin(conf);
    // indexadmin.setTest(true);

    desc = new HTableDescriptor(TableName.valueOf(tableName));
    
    HColumnDescriptor h1=new HColumnDescriptor(Bytes.toBytes("f1"));
    h1.setMaxVersions(10);
    h1.setMinVersions(3);
    desc.addFamily(h1);
    
    
    HColumnDescriptor h2=new HColumnDescriptor(Bytes.toBytes("f2"));
    h2.setMaxVersions(10);
    h2.setMinVersions(3);
    desc.addFamily(h2);
    
    HColumnDescriptor h3=new HColumnDescriptor(Bytes.toBytes("f3"));
    h3.setMaxVersions(10);
    h3.setMinVersions(3);
    desc.addFamily(h3);
    
    HColumnDescriptor h4=new HColumnDescriptor(Bytes.toBytes("f4"));
    h4.setMaxVersions(10);
    h4.setMinVersions(3);
    desc.addFamily(h4);
    
//    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f2")));
//    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f3")));
//    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f4")));

   
    // key ORDERKEY Int
    // c1 CUSTKEY Int
    // c2 ORDERSTATUS String
    // c3 TOTALPRICE Double index
    // c4 ORDERDATE String index
    // c5 ORDERPRIORITY String index
    // c6 CLERK String
    // c7 SHIPPRIORITY Int
    // c8 COMMENT String
    
    
    index = new IndexSpecification[4];
    index[0] = new IndexSpecification(Bytes.toBytes("f1:c2"), IndexType.CCINDEX);
    
    //index[3] = new IndexSpecification(Bytes.toBytes("f1:c4"), IndexType.CCINDEX);
    index[1] = new IndexSpecification(Bytes.toBytes("f1:c1"), IndexType.SECONDARYINDEX);
    
    index[2] = new IndexSpecification(Bytes.toBytes("f2:c3"), IndexType.IMPSECONDARYINDEX);
    index[2].addAdditionColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
    index[2].addAdditionColumn(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
    
    index[3] = new IndexSpecification(Bytes.toBytes("f3:c4"), IndexType.CCINDEX);
    //index[2].addAdditionFamily(Bytes.toBytes("f3"));

    indexDesc = new IndexTableDescriptor(desc, index);
    
    
	 if (admin.tableExists(tableName)) {
	      admin.disableTable(tableName);
	      admin.deleteTable(tableName);
	    }
    
    indexadmin.createTable(indexDesc);

    // table without index
    /*
    newdesc = new HTableDescriptor(TableName.valueOf(newTableName));
    newdesc.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
    newdesc.addFamily(new HColumnDescriptor(Bytes.toBytes("f2")));
    newIndexDesc = new IndexTableDescriptor(newdesc);
    indexadmin.createTable(newIndexDesc);
    */
    System.out.println("Create table finished!");
    return;
  }
}
