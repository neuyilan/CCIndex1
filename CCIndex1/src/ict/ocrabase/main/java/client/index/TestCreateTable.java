package ict.ocrabase.main.java.client.index;

import ict.ocrabase.main.java.client.index.IndexSpecification.IndexType;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCreateTable {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] tableName = Bytes.toBytes("testtable");
  private static final byte[] newTableName = Bytes.toBytes("NewTable");
  private static Configuration conf;
  private static IndexAdmin indexadmin;
  private static IndexTableDescriptor indexDesc;
  private static HBaseAdmin admin;
  private static IndexSpecification[] index;
  private static HTableDescriptor desc;
  private static HTableDescriptor newdesc;
  private static IndexTableDescriptor newIndexDesc;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // TEST_UTIL.startMiniCluster();
    conf = HBaseConfiguration.create();
    indexadmin = new IndexAdmin(conf);
    admin = new HBaseAdmin(conf);
    indexadmin.setTest(true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    // TEST_UTIL.shutdownMiniCluster();
  }

  @SuppressWarnings("deprecation")
  @Before
  public void before() throws IOException, IndexExistedException {
    // table with indexes
    desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f2")));
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f3")));

    index = new IndexSpecification[3];
    index[0] = new IndexSpecification(Bytes.toBytes("f1:c1"));
    index[1] = new IndexSpecification(Bytes.toBytes("f2:c2"), IndexType.CCINDEX);
    index[2] = new IndexSpecification(Bytes.toBytes("f2:c3"), IndexType.IMPSECONDARYINDEX);
    index[2].addAdditionColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
    index[2].addAdditionColumn(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
    index[2].addAdditionFamily(Bytes.toBytes("f3"));

    indexDesc = new IndexTableDescriptor(desc, index);
    indexadmin.createTable(indexDesc);

    // table without index
    newdesc = new HTableDescriptor(newTableName);
    newdesc.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
    newIndexDesc = new IndexTableDescriptor(newdesc);
    indexadmin.createTable(newIndexDesc);
  }

  @After
  public void after() throws IOException {
    for (HTableDescriptor temp : indexadmin.listTables()) {
      indexadmin.disableTable(temp.getName());
      System.out.println("Disable table " + Bytes.toString(temp.getName()));
      indexadmin.deleteTable(temp.getName());
      System.out.println("Drop table " + Bytes.toString(temp.getName()));

    }
  }

  @Test
  public void testCreateTable() throws IOException, IndexExistedException {
    // check if table number match
    HTableDescriptor[] tables = admin.listTables();
    Assert.assertTrue(tables.length == indexDesc.getIndexedColumns().length + 2);
    System.out.println("TEST START!!!");
    // check if each table exists and enable(for table with indexes)
    Assert.assertTrue(indexadmin.tableExists(tableName));
    Assert.assertTrue(indexadmin.isTableEnabled(tableName));
    Assert.assertTrue(admin.tableExists(tableName));
    Assert.assertTrue(admin.isTableEnabled(tableName));
    for (IndexSpecification temp1 : indexDesc.getIndexSpecifications()) {
      Assert.assertTrue(admin.tableExists(temp1.getIndexTableName()));
      Assert.assertTrue(admin.isTableEnabled(temp1.getIndexTableName()));
    }

    // check if each table exists and enable(for table without index)
    Assert.assertTrue(indexadmin.tableExists(newTableName));
    Assert.assertTrue(indexadmin.isTableEnabled(newTableName));
    Assert.assertTrue(admin.tableExists(newTableName));
    Assert.assertTrue(admin.isTableEnabled(newTableName));

    // check if indexes are recorded in table descriptor
    IndexTableDescriptor indexDescNew =
        new IndexTableDescriptor(admin.getTableDescriptor(tableName));
    Assert.assertTrue(indexDesc.equals(indexDescNew));
  }

}
