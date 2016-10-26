package org.apache.hbase.coprocessor;

import ict.ocrabase.main.java.client.index.IndexSpecification;
import ict.ocrabase.main.java.client.index.IndexSpecification.IndexType;
import ict.ocrabase.main.java.client.index.IndexTableDescriptor;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class PutObserver extends BaseRegionObserver {

  @Override
  public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put,
      final WALEdit edit, final Durability durability) throws IOException {

    RegionCoprocessorEnvironment currentRCEnvironment = e.getEnvironment();
    HRegion currentHRegion = currentRCEnvironment.getRegion();
    HTableDescriptor baseTableDesc = currentHRegion.getTableDesc();
    if (Bytes.compareTo(baseTableDesc.getName(), TableName.META_TABLE_NAME.getName()) == 0
        || Bytes.compareTo(baseTableDesc.getName(), TableName.NAMESPACE_TABLE_NAME.getName()) == 0) {
      return;
   // avoid hbase:meta Table and hbase:namespace Table
    }

    IndexTableDescriptor baseTableIndexDesc = new IndexTableDescriptor(baseTableDesc);

    if (baseTableIndexDesc.hasIndex()) {
      Configuration tempConf = currentRCEnvironment.getConfiguration();
      for (IndexSpecification indexSpec : baseTableIndexDesc.getIndexSpecifications()) {
        byte[] indexTableName = indexSpec.getIndexTableName();
        HTable tempIndexTable = new HTable(tempConf, indexTableName);
        // for every index table, put proper data into it.

        byte[] indexRowKey = baseTableIndexDesc.getKeyGenerator().createIndexRowKey(indexSpec, put);

        if (indexRowKey == null) continue;

        Put tempPut = new Put(indexRowKey);
        // CCIndex
        if (indexSpec.getIndexType() == IndexType.CCINDEX) {
          for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
            if (Bytes.compareTo(entry.getKey(), indexSpec.getFamily()) == 0) {
              // same family with index column;
              for (Cell cellItr : entry.getValue()) {
                if (Bytes.compareTo(CellUtil.cloneQualifier(cellItr), indexSpec.getQualifier()) != 0) {
                  tempPut.add(CellUtil.cloneFamily(cellItr), CellUtil.cloneQualifier(cellItr),
                    cellItr.getTimestamp(), CellUtil.cloneValue(cellItr));
                } else if (put.size() == 1) {

                  tempPut.add(CellUtil.cloneFamily(cellItr), null, cellItr.getTimestamp(),
                    CellUtil.cloneValue(cellItr));
                }
              }
            } else {
              for (Cell cellItr : entry.getValue()) {
                tempPut.add(CellUtil.cloneFamily(cellItr), CellUtil.cloneQualifier(cellItr),
                  cellItr.getTimestamp(), CellUtil.cloneValue(cellItr));
              }
            }
          }
        } else if (indexSpec.getIndexType() == IndexType.IMPSECONDARYINDEX) {
          Map<byte[], Set<byte[]>> additionMap = indexSpec.getAdditionMap();

          for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
            if (additionMap.containsKey(entry.getKey())) {
              Set<byte[]> columnSet = additionMap.get(entry.getKey());

              // family that index cloumn belongs to
              if (Bytes.compareTo(indexSpec.getFamily(), entry.getKey()) == 0) {
                // addition family
                if (columnSet == null || columnSet.size() == 0) {
                  for (Cell cellItr : entry.getValue()) {
                    if (Bytes.compareTo(CellUtil.cloneQualifier(cellItr), indexSpec.getQualifier()) != 0) {
                      tempPut.add(CellUtil.cloneFamily(cellItr), CellUtil.cloneQualifier(cellItr),
                        cellItr.getTimestamp(), CellUtil.cloneValue(cellItr));
                    } else if (put.size() == 1) {
                      // only include index column
                      tempPut.add(CellUtil.cloneFamily(cellItr), null, cellItr.getTimestamp(),
                        CellUtil.cloneValue(cellItr));
                    }
                  }
                } else {
                  for (Cell cellItr : entry.getValue()) {
                    if (columnSet.contains(CellUtil.cloneQualifier(cellItr))) {
                      if (Bytes.compareTo(CellUtil.cloneQualifier(cellItr),
                        indexSpec.getQualifier()) != 0) {
                        tempPut.add(CellUtil.cloneFamily(cellItr),
                          CellUtil.cloneQualifier(cellItr), cellItr.getTimestamp(),
                          CellUtil.cloneValue(cellItr));
                      } else if (put.size() == 1) {
                        // only include index column
                        tempPut.add(CellUtil.cloneFamily(cellItr), null, cellItr.getTimestamp(),
                          CellUtil.cloneValue(cellItr));
                      }
                    }
                  }
                }
              } else {
                // addition family
                if (columnSet == null || columnSet.size() == 0) {
                  for (Cell cellItr : entry.getValue()) {
                    tempPut.add(CellUtil.cloneFamily(cellItr), CellUtil.cloneQualifier(cellItr),
                      cellItr.getTimestamp(), CellUtil.cloneValue(cellItr));
                  }
                } else {
                  for (Cell cellItr : entry.getValue()) {
                    if (columnSet.contains(CellUtil.cloneQualifier(cellItr))) {
                      tempPut.add(CellUtil.cloneFamily(cellItr), CellUtil.cloneQualifier(cellItr),
                        cellItr.getTimestamp(), CellUtil.cloneValue(cellItr));
                    }
                  }
                }
              }

            }
          }
        } else {
          tempPut.add(indexSpec.getFamily(), null, null);
        }

        try {
          tempIndexTable.put(tempPut);
        } catch (RetriesExhaustedWithDetailsException e1) {
          // TODO Auto-generated catch block

          e1.printStackTrace();
        } catch (InterruptedIOException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        tempIndexTable.close();

      }
    }
  }

}
