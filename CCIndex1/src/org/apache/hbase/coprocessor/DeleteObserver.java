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
import org.apache.hadoop.hbase.client.Delete;
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

public class DeleteObserver extends BaseRegionObserver {

  @Override
  public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> e, final Delete delete,
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
        // for every index table, delete proper data into it.

        byte[] indexRowKey = baseTableIndexDesc.getKeyGenerator().createIndexRowKey(indexSpec, delete);

        if (indexRowKey == null) continue;

        Delete tempDelete = new Delete(indexRowKey);
        // CCIndex
        if (indexSpec.getIndexType() == IndexType.CCINDEX) {
          for (Map.Entry<byte[], List<Cell>> entry : delete.getFamilyCellMap().entrySet()) {
            if (Bytes.compareTo(entry.getKey(), indexSpec.getFamily()) == 0) {
              // same family with index column;
              for (Cell cellItr : entry.getValue()) {
                if (Bytes.compareTo(CellUtil.cloneQualifier(cellItr), indexSpec.getQualifier()) != 0) {
                	tempDelete.deleteColumn(CellUtil.cloneFamily(cellItr), CellUtil.cloneQualifier(cellItr),
                    cellItr.getTimestamp());
                } else if (delete.size() == 1) {
                  tempDelete.deleteColumn(CellUtil.cloneFamily(cellItr), null, cellItr.getTimestamp());
                }
              }
            } else {
              for (Cell cellItr : entry.getValue()) {
                tempDelete.deleteColumn(CellUtil.cloneFamily(cellItr), CellUtil.cloneQualifier(cellItr),
                        cellItr.getTimestamp());
                
              }
            }
          }
        } else if (indexSpec.getIndexType() == IndexType.IMPSECONDARYINDEX) {
          Map<byte[], Set<byte[]>> additionMap = indexSpec.getAdditionMap();

          for (Map.Entry<byte[], List<Cell>> entry : delete.getFamilyCellMap().entrySet()) {
            if (additionMap.containsKey(entry.getKey())) {
              Set<byte[]> columnSet = additionMap.get(entry.getKey());

              // family that index cloumn belongs to
              if (Bytes.compareTo(indexSpec.getFamily(), entry.getKey()) == 0) {
                // addition family
                if (columnSet == null || columnSet.size() == 0) {
                  for (Cell cellItr : entry.getValue()) {
                    if (Bytes.compareTo(CellUtil.cloneQualifier(cellItr), indexSpec.getQualifier()) != 0) {
                    	delete.deleteColumn(CellUtil.cloneFamily(cellItr), CellUtil.cloneQualifier(cellItr),
                        cellItr.getTimestamp());
                    } else if (delete.size() == 1) {
                      // only include index column
                      delete.deleteColumn(CellUtil.cloneFamily(cellItr), null, cellItr.getTimestamp());
                    }
                  }
                } else {
                  for (Cell cellItr : entry.getValue()) {
                    if (columnSet.contains(CellUtil.cloneQualifier(cellItr))) {
                      if (Bytes.compareTo(CellUtil.cloneQualifier(cellItr),
                        indexSpec.getQualifier()) != 0) {
                       tempDelete.deleteColumn(CellUtil.cloneFamily(cellItr),
                          CellUtil.cloneQualifier(cellItr), cellItr.getTimestamp());
                      } else if (delete.size() == 1) {
                        // only include index column
                        tempDelete.deleteColumn(CellUtil.cloneFamily(cellItr), null, cellItr.getTimestamp());
                      }
                    }
                  }
                }
              } else {
                // addition family
                if (columnSet == null || columnSet.size() == 0) {
                  for (Cell cellItr : entry.getValue()) {
                    tempDelete.deleteColumn(CellUtil.cloneFamily(cellItr),
                            CellUtil.cloneQualifier(cellItr), cellItr.getTimestamp());
                  }
                } else {
                  for (Cell cellItr : entry.getValue()) {
                    if (columnSet.contains(CellUtil.cloneQualifier(cellItr))) {
                      tempDelete.deleteColumn(CellUtil.cloneFamily(cellItr),
                              CellUtil.cloneQualifier(cellItr), cellItr.getTimestamp());
                    }
                  }
                }
              }

            }
          }
        } else {
          tempDelete.deleteColumn(indexSpec.getFamily(), null);
        }

        try {
          tempIndexTable.delete(tempDelete);
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
