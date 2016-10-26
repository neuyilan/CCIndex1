package ict.ocrabase.main.java.client.index;

import ict.ocrabase.main.java.client.index.IndexSpecification.IndexType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Singleton class for index maintence logic.
 * 
 * @author wanhao
 */
public class IndexUtils {
	
	public static IndexDelete createIndexDelete(IndexTableDescriptor indexDesc, Delete delete, Result result) throws IOException {
		IndexDelete indexdelete=new IndexDelete();
		indexdelete.addDelete(IndexConstants.KEY, delete);
		
		if(result!=null && !result.isEmpty()){
			for(IndexSpecification indexSpec:indexDesc.getIndexSpecifications()){
				byte[] rowkey=indexDesc.getKeyGenerator().createIndexRowKey(indexSpec, result);
				
				if(rowkey!=null){
					Delete temp = new Delete(rowkey);
					
					boolean delWholeRow=false;
					
					if(delete.isEmpty()){
						delWholeRow=true;
					}else{
						if(delete.getFamilyMap().containsKey(indexSpec.getFamily())){
							for(KeyValue kv:delete.getFamilyMap().get(indexSpec.getFamily())){
								if(kv.getQualifierLength()==0){
									if(Bytes.compareTo(kv.getFamily(), indexSpec.getFamily())==0){
										delWholeRow=true;
										break;
									}
								}else{
									if(Bytes.compareTo(kv.getFamily(), indexSpec.getFamily())==0 &&
											Bytes.compareTo(kv.getQualifier(), indexSpec.getQualifier())==0){
										delWholeRow=true;
										break;
									}
								}
							}
						}
					}
					
					if (!delWholeRow) {
						if (indexSpec.getIndexType() == IndexType.CCINDEX) {
							for (Map.Entry<byte[], List<KeyValue>> entry : delete
									.getFamilyMap().entrySet()) {
								for (KeyValue kv : entry.getValue()) {
									if (kv.getQualifierLength()==0) {
										temp.deleteFamily(kv.getFamily());
									}else{
									temp.deleteColumn(kv.getFamily(),kv.getQualifier());
									}
								}
							}

						} else if (indexSpec.getIndexType() == IndexType.IMPSECONDARYINDEX) {
							for (Map.Entry<byte[], List<KeyValue>> entry : delete.getFamilyMap().entrySet()) {
								for (KeyValue kv : entry.getValue()) {
									if (kv.getQualifierLength()==0) {
										if (indexSpec.getAdditionMap().containsKey(kv.getFamily())) {
											temp.deleteFamily(kv.getFamily());
										}
									} else {
										if (indexSpec.getAdditionMap().containsKey(kv.getFamily())) {
											Set<byte[]> qua = indexSpec.getAdditionMap().get(kv.getFamily());
											if (qua == null || qua.contains(kv.getQualifier())) {
												temp.deleteColumn(kv.getFamily(),kv.getQualifier());
											}
										}
									}
								}
							}
						}

					}
					
					indexdelete.addDelete(indexSpec.getIndexColumn(), temp);
				}
			}
		}
		
		return indexdelete;
	}

}
