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

public class CreateTableTestBulkload {
//  private static final byte[] tableName = Bytes.toBytes("test4");  //large_table     test_table_index      test_table_orderkey 
//	private static final byte[] tableName = Bytes.toBytes("test_table_index_3_f4");        //large_table_f4  test_table_index_f4   test_table_f5
	private static final byte[] tableName = Bytes.toBytes("test_table");
	private static Configuration conf;
  private static IndexAdmin indexadmin;
  private static HBaseAdmin admin;
  private static HTableDescriptor desc;

  public static void main(String[] args) throws IOException, IndexExistedException {
    conf = HBaseConfiguration.create();
    indexadmin = new IndexAdmin(conf);
    admin = new HBaseAdmin(conf);

    desc = new HTableDescriptor(TableName.valueOf(tableName));
    
   
    
    
    for(int i=0;i<2;i++){
    	 HColumnDescriptor h1=new HColumnDescriptor(Bytes.toBytes("f"+(i+1)));
//    	 h1.setMaxVersions(10);
//    	 h1.setMinVersions(3);
    	 desc.addFamily(h1);
    }

   
//    
//    index = new IndexSpecification[4];
//    index[0] = new IndexSpecification(Bytes.toBytes("f1:c1"), IndexType.CCINDEX);
//    
//    //index[3] = new IndexSpecification(Bytes.toBytes("f1:c4"), IndexType.CCINDEX);
//    index[1] = new IndexSpecification(Bytes.toBytes("f1:c1"), IndexType.SECONDARYINDEX);
//    
//    index[2] = new IndexSpecification(Bytes.toBytes("f2:c3"), IndexType.IMPSECONDARYINDEX);
//    index[2].addAdditionColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
//    index[2].addAdditionColumn(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
//    
//    index[3] = new IndexSpecification(Bytes.toBytes("f3:c4"), IndexType.CCINDEX);
//    //index[2].addAdditionFamily(Bytes.toBytes("f3"));
//
//    indexDesc = new IndexTableDescriptor(desc, index);
    
    
	 if (admin.tableExists(tableName)) {
	      admin.disableTable(tableName);
	      admin.deleteTable(tableName);
	    }
    
    indexadmin.createTable(desc);
//	 admin.createTable(desc);

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
