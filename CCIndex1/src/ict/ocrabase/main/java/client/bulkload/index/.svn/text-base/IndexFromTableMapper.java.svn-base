package ict.ocrabase.main.java.client.bulkload.index;

import ict.ocrabase.main.java.client.bulkload.BulkLoadUtil;
import ict.ocrabase.main.java.client.bulkload.ImportConstants;
import ict.ocrabase.main.java.client.bulkload.KeyValueArray;
import ict.ocrabase.main.java.client.bulkload.TableInfo;
import ict.ocrabase.main.java.client.index.IndexKeyGenerator;
import ict.ocrabase.main.java.client.index.IndexNotExistedException;
import ict.ocrabase.main.java.client.index.IndexSpecification;
import ict.ocrabase.main.java.client.index.IndexTableDescriptor;
import ict.ocrabase.main.java.client.index.SimpleIndexKeyGenerator;
import ict.ocrabase.main.java.client.index.IndexSpecification.IndexType;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Used for adding index, transfer the rowkey into the index table rowkey
 * @author gu
 *
 */
public class IndexFromTableMapper extends
Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, KeyValueArray>{
	private TableInfo table;
	private List<IndexColumn> indexColumns = new ArrayList<IndexColumn>();
	private IndexKeyGenerator ikg;
	private long records;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		table = new TableInfo(context.getConfiguration().get(
				ImportConstants.BULKLOAD_DATA_FORMAT));
		byte[] htdBytes = Bytes.toBytesBinary(context.getConfiguration().get("bulkload.indexTableDescriptor"));
		HTableDescriptor htd = new HTableDescriptor();
		htd.readFields(new DataInputStream(new ByteArrayInputStream(htdBytes)));
		IndexTableDescriptor itd = new IndexTableDescriptor(htd);
		
		//get indexColumns
		for(TableInfo.ColumnInfo col : table.getColumnInfo()){
			try {
				indexColumns.add(new IndexColumn(itd, Bytes.toString(col.getFamily()), Bytes.toString(col.getQualifier())));
			} catch (IndexNotExistedException e) {
				e.printStackTrace();
			}
		}
		
		//get indexKeyGenerator
		try {
			Constructor<?> cons = context.getConfiguration().getClass("bulkload.indexKeyGenerator", SimpleIndexKeyGenerator.class).getConstructor();
			ikg = (IndexKeyGenerator)cons.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		records = 0;
	}
	
	protected void map(ImmutableBytesWritable key, Result result,
			Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, KeyValueArray>.Context context) throws IOException, InterruptedException {
		int count = 1;
		
		if(records%10000 == 0)
			context.setStatus("Write "+records);
		records++;
		
		for(IndexColumn col : indexColumns){
			KeyValue rowKV = result.getColumnLatest(col.family, col.qualifier);
			if(rowKV == null || rowKV.getValueLength() == 0){
				count++;
				continue;
			}
			
			byte[] rowkey = new byte[key.getLength()];
			System.arraycopy(key.get(), key.getOffset(), rowkey, 0, key.getLength());
			byte[] row = ikg.createIndexRowKey(rowkey, rowKV.getValue());
			
			ImmutableBytesWritable indexKey = new ImmutableBytesWritable(
					Bytes.add(new byte[]{(byte) count}, row));

			TreeSet<KeyValue> kvList = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
			
			switch(col.indexType){
			case CCINDEX:
				for(KeyValue kv : result.raw()){
					if(Bytes.compareTo(kv.getFamily(), col.family) == 0 && Bytes.compareTo(kv.getQualifier(), col.qualifier) == 0)
						continue;
					kvList.add(createNewKV(kv, row));
				}
				if(kvList.size() == 0){
					//create a empty column
					kvList.add(new KeyValue(BulkLoadUtil.createKVByte(row, col.emptyColumn, null, 0, 0)));
				}
				break;
			case SECONDARYINDEX:
				kvList.add(new KeyValue(BulkLoadUtil.createKVByte(row, col.emptyColumn, null, 0, 0)));
				break;
			case IMPSECONDARYINDEX:
				for(KeyValue kv : result.raw()){
					if(!TableInfo.belongsToAdditionCol(kv.getFamily(), kv.getQualifier(), col.additionCol))
						continue;
					kvList.add(createNewKV(kv, row));
				}
				if(kvList.size() == 0){
					//create a empty column
					kvList.add(new KeyValue(BulkLoadUtil.createKVByte(row, col.emptyColumn, null, 0, 0)));
				}
				break;
			}
			
			context.write(indexKey, new KeyValueArray(kvList));
			count++;
		
		}
	}
	
	/**
	 * Replace the given KeyValue with the new rowkey
	 * @param kv
	 * @param row
	 * @return the new KeyValue
	 */
	private KeyValue createNewKV(KeyValue kv, byte[] row) {
		byte[] kvBytes = new byte[row.length + kv.getLength()-kv.getRowLength()];
		int p = Bytes.putInt(kvBytes, 0, kv.getKeyLength()-kv.getRowLength()+row.length);
		p = Bytes.putInt(kvBytes, p, kv.getValueLength());
		p = Bytes.putShort(kvBytes, p, (short)(row.length & 0x0000ffff));
		p = Bytes.putBytes(kvBytes, p, row, 0, row.length);
		p = Bytes.putBytes(kvBytes, p, kv.getBuffer(), kv.getOffset()+10+kv.getRowLength(), kv.getLength()-10-kv.getRowLength());
		return new KeyValue(kvBytes, 0, kvBytes.length);
	}

	/**
	 * Store some index info
	 * @author gu
	 *
	 */
	class IndexColumn{
		public byte[] family;
		public byte[] qualifier;
		public byte[] emptyColumn;
		public Map<byte[],Set<byte[]>> additionCol = null;
		public IndexType indexType;
		
		public IndexColumn(IndexTableDescriptor des, String family, String qualifier) throws IndexNotExistedException{
			this.family = Bytes.toBytes(family);
			this.qualifier = Bytes.toBytes(qualifier);
			
			IndexSpecification indexSpe = des.getIndexSpecification(Bytes.toBytes(family+":"+qualifier));
			IndexType type = indexSpe.getIndexType();
			switch(type){
			case CCINDEX:
				emptyColumn = getEmptyColumnByte(Bytes.toBytes(family), Bytes.toBytes(""));
				indexType = IndexType.CCINDEX;
				break;
			case SECONDARYINDEX:
				emptyColumn = getEmptyColumnByte(Bytes.toBytes(family), Bytes.toBytes(""));
				indexType = IndexType.SECONDARYINDEX;
				break;
			case IMPSECONDARYINDEX:
				additionCol = indexSpe.getAdditionMap();
				if(additionCol.size() == 0){
					emptyColumn = getEmptyColumnByte(Bytes.toBytes(family), Bytes.toBytes(""));
					indexType = IndexType.SECONDARYINDEX;
				}else{
					emptyColumn = getEmptyColumnByte(additionCol.keySet().iterator().next(), Bytes.toBytes(""));
					indexType = IndexType.IMPSECONDARYINDEX;
				}
				break;
			default:
				throw new IndexNotExistedException("Not support index type.");
			}
		}
		
		private byte[] getEmptyColumnByte(byte[] family, byte[] qualifier){
			
			int length = 2*Bytes.SIZEOF_BYTE + family.length + qualifier.length + Bytes.SIZEOF_LONG;
			byte[] bytes = new byte[length];
			int pos=0;
			pos = Bytes.putByte(bytes, pos, (byte)(family.length & 0x0000ff));
			pos = Bytes.putBytes(bytes, pos, family, 0, family.length);
			pos = Bytes.putBytes(bytes, pos, qualifier, 0, qualifier.length);
			pos = Bytes.putLong(bytes, pos, System.currentTimeMillis());
			pos = Bytes.putByte(bytes, pos, Type.Put.getCode());
			return bytes;
		}
	}
}
