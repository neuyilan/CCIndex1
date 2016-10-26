package ict.ocrabase.main.java.client.bulkload;

import ict.ocrabase.main.java.client.bulkload.noindex.HFileOutput;
import ict.ocrabase.main.java.client.index.IndexAdmin;
import ict.ocrabase.main.java.client.index.IndexKeyGenerator;
import ict.ocrabase.main.java.client.index.IndexNotExistedException;
import ict.ocrabase.main.java.client.index.IndexSpecification;
import ict.ocrabase.main.java.client.index.IndexTableDescriptor;
import ict.ocrabase.main.java.client.index.IndexSpecification.IndexType;
import ict.ocrabase.main.java.regionserver.DataType;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * Describe the table information, includes 
 * table name, separator, column name, column type and index
 * @author gu
 *
 */
public class TableInfo {

	private String name;
	private byte separator;
	private List<ColumnInfo> columns = new ArrayList<ColumnInfo>();
	private int[] indexPos = null;
	static final Log LOG = LogFactory.getLog(TableInfo.class);

	/**
	 * Create TableInfo from a xml file
	 * @param fileName the xml file name
	 * @return TableInfo
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static TableInfo createFromXML(String fileName) throws Exception{
		File file = new File(fileName);
		if(!file.exists()){
			URL defaultURL = TableInfo.class.getClassLoader().getResource("table.xml");
			if(defaultURL == null)
				throw new Exception("Could not found in "+fileName+" ,and also could not found table.xml");
			else
				file = new File(defaultURL.getPath());
		}
		InputStream in = new FileInputStream(file);
		
		TableInfo tab = new TableInfo();
		
		SAXReader saxReader = new SAXReader();
		Document document = saxReader.read(in);
		Element t = document.getRootElement().element("table");
		tab.name = t.attributeValue("name");
		
		List<Element> famList = t.elements("family");
		for(Element fam : famList){
			String famName = fam.attributeValue("name");
			List<Element> quaList = fam.elements("qualifier");
			for(Element qua : quaList){
				String quaName = qua.attributeValue("name");
				String dataType = qua.attributeValue("type",DataType.STRING.toString());
				tab.addColumn(famName, quaName, null, null, dataType);
			}
		}
		String sep = document.getRootElement().elementText("separator");
		if (sep.length() == 2) {
			if (sep.equals("\\t")) {
				sep = "\t";
			} else {
				throw new Exception("Could not handle sparator: " + sep);
			}
		}
			
		tab.separator = (byte) sep.charAt(0);
		
		return tab;
	}
	
	/**
	 * Construct TableInfo with table name, data format and separator
	 * @param tableName the name of the table
	 * @param columnList data format
	 * @param s separator
	 */
	public TableInfo(String tableName,Map<String, ArrayList<String>> columnList, byte s){
		this.name = tableName;
		this.separator = s;
		for(Map.Entry<String, ArrayList<String>> column : columnList.entrySet()){
			String col = column.getKey();
			
			ArrayList<String> names = column.getValue();
			String[] colName = col.split(":");
			if(colName[0].equalsIgnoreCase(ImportConstants.KEY) && colName.length == 1){
				ColumnInfo colInfo = new ColumnInfo(ImportConstants.KEY, null, null, null, DataType.STRING.toString());
				columns.add(colInfo);
			} else if(colName[0].equalsIgnoreCase(ImportConstants.TIMESTAMP) && colName.length == 1){
				ColumnInfo colInfo = new ColumnInfo(ImportConstants.TIMESTAMP, null, null, null, DataType.STRING.toString());
				columns.add(colInfo);
			} else {
				String dataType = colName.length==3?colName[2]:DataType.STRING.toString();
				ColumnInfo colInfo;
				if(names == null){
					colInfo = new ColumnInfo(colName[0],colName[1],null,null,dataType);
				}else {
					colInfo = new ColumnInfo(colName[0],colName[1],names.get(0),names.get(1),dataType);
				}
				columns.add(colInfo);
			}
			
		}
		
//		generateIndexPos();
	}
	
	
	/**
	 * Construct TableInfo with a table info string, it contains 
	 * table name, separator, column info and index, 
	 * eg. test,9,f:1:String:CCINDEX:test_f_1_CCIT
	 * @param info
	 */
	public TableInfo(String info){
		
//////////////////////////////qihouliang-test/////////////////////////////
//		info="table,tab,KEY,CF1:col1dffd:int:C1F1:co111l1dffd:int11,CfdfF2:cdfol6:string,TIMESTAMP";
//		"table,tab,KEY,CF1:col1:int,CF2:col6:string,TIMESTAMP"
//////////////////////////////qihouliang-test/////////////////////////////
		String[] token = info.split(",");
		this.name = token[0];
		if(token[1].equalsIgnoreCase("comma"))
			this.separator = (byte)',';
		else if(token[1].equalsIgnoreCase("tab"))
			this.separator = (byte)'\t';
		else if(token[1].equalsIgnoreCase("semicolon"))
			this.separator = (byte)';';
		else if(token[1].equalsIgnoreCase("space"))
			this.separator = (byte)' ';
		else {
			int s = Integer.parseInt(token[1]);
			this.separator = (byte) (char) s;
		}
		for(int i = 2;i < token.length;i++){
			String[] col = token[i].split(":", 5);
			
//////////////////////////////qihouliang-test/////////////////////////////
//			for(int m =0;m<col.length;m++){
//				System.out.println(col[m]);
//			}
//			System.out.println("col.length----->"+col.length);
//////////////////////////////qihouliang-test/////////////////////////////
			ColumnInfo colInfo;
			if(col[0].equalsIgnoreCase(ImportConstants.KEY) && col.length == 1){
				colInfo = new ColumnInfo(ImportConstants.KEY, null, null, null, DataType.STRING.toString());
			}else if(col[0].equalsIgnoreCase(ImportConstants.TIMESTAMP)  && col.length == 1){
				colInfo = new ColumnInfo(ImportConstants.TIMESTAMP, null, null, null, DataType.STRING.toString());
			}else {
				String dataType = col.length>=3?col[2]:DataType.STRING.toString();
				if(col.length == 3 || col.length == 2)
					colInfo = new ColumnInfo(col[0],col[1],null,null,dataType);
				else
					colInfo = new ColumnInfo(col[0],col[1],col[3],col[4],dataType);				
			}
			columns.add(colInfo);
		}
		
//		generateIndexPos();
	}
	
	/**
	 * Do nothing
	 */
	public TableInfo() {}
	
	/**
	 * Add index info which is from IndexAdmin into TableInfo
	 * @param conf
	 * @throws IOException
	 */
	public void addIndexInfo(Configuration conf) throws IOException{
		IndexAdmin indexAdmin = new IndexAdmin(conf);
		IndexTableDescriptor des = indexAdmin.getIndexTableDescriptor(Bytes.toBytes(name));
		IndexSpecification[] indexList = des.getIndexSpecifications();
		for(IndexSpecification is : indexList){
			ColumnInfo col = getColumnInfoByName(Bytes.toString(is.getIndexColumn()));
			if (col != null) {
				col.addIndex(is.getIndexType().toString(), is.getIndexTableNameAsString());
			}
		}
	}
	
	public void addDataType(Configuration conf) throws IOException{
		IndexAdmin indexAdmin = new IndexAdmin(conf);
		IndexTableDescriptor des = indexAdmin.getIndexTableDescriptor(Bytes.toBytes(this.name));
		HTableDescriptor htd = des.getTableDescriptor();
		String tableStr = htd.getValue(ImportConstants.DATA_FORMAT);
		TableInfo info = new TableInfo("null,1,"+tableStr);
		ColumnInfo temp;
		for(ColumnInfo col : this.columns){
			if((temp = info.getColumnInfoByName(Bytes.toString(col.getFamily())+":"+Bytes.toString(col.getQualifier()))) != null){
				col.setDataType(temp.getDataType().toString());
			}
		}
	}
	
	/**
	 * Convert current TableInfo into index table info which belongs to column family:qualifier
	 * @param des
	 * @param family
	 * @param qualifier
	 * @return indexType
	 * @throws IndexNotExistedException
	 */
	public IndexType convertToIndexTableInfo(IndexTableDescriptor des, String family, String qualifier) throws IndexNotExistedException{
		IndexSpecification indexSpe = des.getIndexSpecification(Bytes.toBytes(family+":"+qualifier));
		IndexType type = indexSpe.getIndexType();

		switch(type){
		case CCINDEX:
			columns.remove(getColumnInfoByName(family+":"+qualifier));
			//if import data has only one column
			if(columns.size() == 0){
				this.addColumn(family, "", null, null, DataType.STRING.toString());
				return IndexType.SECONDARYINDEX;
			}else
				return IndexType.CCINDEX;
		case SECONDARYINDEX:
			columns.clear();
			this.addColumn(family, "", null, null, DataType.STRING.toString());
			return IndexType.SECONDARYINDEX;
		case IMPSECONDARYINDEX:
			Map<byte [], Set<byte[]>> addition = indexSpe.getAdditionMap();
			if(addition.size() == 0){
				columns.clear();
				this.addColumn(family, "", null, null, DataType.STRING.toString());
				return IndexType.SECONDARYINDEX;
			}else{
				//remove columns
				Iterator<ColumnInfo> it = columns.iterator();
				ColumnInfo col = null;
				while(it.hasNext()){
					col = it.next();
					if(!belongsToAdditionCol(col.family,col.qualifier,addition))
						it.remove();
				}
				if(columns.size() != 0)
					return IndexType.IMPSECONDARYINDEX;
				else{
					this.addColumn(Bytes.toString(addition.keySet().iterator().next()), "", null, null, DataType.STRING.toString());
					return IndexType.SECONDARYINDEX;
				}
			}
		default:
			throw new IndexNotExistedException("Not support index type.");
		}
	}
	
	/**
	 * To check whether the column family:qualifier belongs to the addition map
	 * @param family
	 * @param qualifier
	 * @param addition
	 * @return true if the column belongs to the addition map, false otherwise.
	 */
	public static boolean belongsToAdditionCol(byte[] family, byte[] qualifier,
			Map<byte[], Set<byte[]>> addition) {
		if(!addition.containsKey(family))
			return false;
		else{
			Set<byte[]> valueSet = addition.get(family);
			if(valueSet == null)
				return true;
			else{
				if(valueSet.contains(qualifier))
					return true;
				else
					return false;
//				for(byte[] value : valueSet){
//					if(Bytes.compareTo(qualifier, value) == 0)
//						return true;
//				}
//				return false;
			}
		}
	}
	
	/**
	 * Write IndexTableDescriptor and IndexKeyGenerator into configuration
	 * @param conf
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void writeIndexToConfig(Configuration conf) throws IOException, InterruptedException{
		TableInfo tableWithoutKey = new TableInfo(this.toString());
		tableWithoutKey.removeKey();
		conf.set(ImportConstants.BULKLOAD_DATA_FORMAT, tableWithoutKey.toString());
		conf.set(ImportConstants.BULKLOAD_DATA_FORMAT_WITH_KEY, this.toString());
		IndexAdmin indexAdmin = new IndexAdmin(conf);
		
		//make sure this table enabled
		if(!indexAdmin.isTableEnabled(name))
			indexAdmin.enableTable(name);
		
		IndexTableDescriptor des = indexAdmin.getIndexTableDescriptor(Bytes.toBytes(name));
		
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		des.getTableDescriptor().write(new DataOutputStream(bout));
		conf.set("bulkload.indexTableDescriptor", Bytes.toStringBinary(bout.toByteArray()));
		
		conf.setClass("bulkload.indexKeyGenerator", des.getKeygenClass(), IndexKeyGenerator.class);
		
		HTable table = new HTable(conf, this.name);
		HFileOutput.configureCompression(table, conf);
		
		//set data type to table descriptor
		HBaseAdmin admin = indexAdmin.getHBaseAdmin();
		HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(this.name));
		
		String oldInfo = htd.getValue(ImportConstants.DATA_FORMAT);
		String tableStr;
		if(oldInfo != null){
			String thisStr = this.toSimpleString();
			if(oldInfo.equals(thisStr)){
				return ;
			}
			TableInfo tab = new TableInfo("null,1,"+oldInfo);
			tab.add(this);
			tableStr = tab.toSimpleString();
		}else
			tableStr = this.toSimpleString();
		
		htd.setValue(ImportConstants.DATA_FORMAT, tableStr);
		
		int maxRetryTimes = conf.getInt("hbase.client.retries.number", 10);
		long millis = conf.getLong("hbase.client.pause", 1000);
		int times = 0;
		while(!admin.isTableDisabled(this.name)){
			try{
				admin.disableTable(this.name);
			} catch(IOException e){
				if(times > maxRetryTimes){
					throw e;
				}else {
					LOG.error("Unable to disable table "+this.name+" "+times+" times, try again later...");
					Thread.sleep(millis);
				}
			}
			times ++;
		}
		
		admin.modifyTable(Bytes.toBytes(this.name), htd);
		
		times = 0;
		while(!admin.isTableEnabled(this.name)){
			try{
				admin.enableTable(this.name);
			} catch(IOException e){
				if(times > maxRetryTimes){
					throw e;
				}else {
					LOG.error("Unable to disable table "+this.name+" "+times+" times, try again later...");
					Thread.sleep(millis);
				}
			}
			times ++;
		}
		
	}

	/**
	 * Add all info's column into TableInfo
	 * @param info
	 */
	private void add(TableInfo info) {
		for(ColumnInfo col : info.columns){
			if(this.getColumnInfoByName(Bytes.toString(col.getFamily())+":"+Bytes.toString(col.getQualifier())) == null){
				this.addColumn(Bytes.toString(col.getFamily()), Bytes.toString(col.getQualifier()), col.getIndex(), col.getIndexTableName(), col.getDataType().toString());
			}
		}
	}

	/**
	 * Add a column to TableInfo
	 * @param familyName
	 * @param qualifierName
	 * @param indexName
	 * @param indexTableName
	 * @param cctTableName
	 * @param DataType
	 */
	public void addColumn(String familyName, String qualifierName, String indexName, String indexTableName, String DataType){
		ColumnInfo col = new ColumnInfo(familyName, qualifierName, indexName, indexTableName, DataType);
		columns.add(col);
	}
	
	public void deleteFamily(String familyName) {
		for(Iterator<ColumnInfo> it=columns.iterator();it.hasNext();){
			if(familyName.equals(Bytes.toStringBinary(it.next().getFamily())))
				it.remove();
		}
		generatePos();
	}

	/**
	 * Get all the position of the column which has the index
	 */
	private void generateIndexPos(){
		ArrayList<Integer> pos = new ArrayList<Integer>();
        for(ColumnInfo ci : columns){
        	if(!ci.getIndex().startsWith("null"))
        		pos.add(ci.getPos());
        }
        indexPos = new int[pos.size()];
        int j = 0;
        for(int p : pos){
        	indexPos[j] = p;
        	j++;
        }
	}
	
	private void generatePos() {
		for(int i=0;i<columns.size();i++){
			columns.get(i).setPos(i);
		}
	}
	
	/**
	 * Convert to table info string
	 */
	public String toString(){
		String table = this.name ;
		table += "," + (int)(char)this.getSeparator();
		for(ColumnInfo column : this.columns){
			if(ImportConstants.KEY.equals(Bytes.toString(column.family)) && column.qualifier == null){
				table += ","+ImportConstants.KEY;
			} else if(ImportConstants.TIMESTAMP.equals(Bytes.toString(column.family)) && column.qualifier == null) {
				table += ","+ImportConstants.TIMESTAMP;
			} else {
				table += "," + Bytes.toString(column.family) + ":" + Bytes.toString(column.qualifier) + ":" + column.dataType+ ":"+ (column.index==null?"null":column.index)
				+":"+ (column.indexTableName==null?"null":column.indexTableName);
			}
		}
		return table;
	}
	
	public String toSimpleString(){
		StringBuilder table = new StringBuilder();
		for(ColumnInfo column : this.columns){
			if( (ImportConstants.KEY.equals(Bytes.toString(column.family)) || ImportConstants.TIMESTAMP.equals(Bytes.toString(column.family))) && column.qualifier == null){
				continue;
			}
			table.append("," + Bytes.toString(column.family) + ":" + Bytes.toString(column.qualifier) + ":" + column.dataType); 
		}
		table.replace(0, 1, "");
		return table.toString();
	}
	
	/**
	 * Get data format, it represents by the map between the column name and the index info
	 * @return the map between column name and index info
	 */
	public Map<String,ArrayList<String>> getDataFormat(){
		Map<String, ArrayList<String>> dataFormat = new LinkedHashMap<String, ArrayList<String>>();
		for(ColumnInfo column : this.columns){
			String key = Bytes.toString(column.family) + ":" + Bytes.toString(column.qualifier) + ":" + column.dataType;
			if(column.index==null)
				dataFormat.put(key, null);
			else{
				ArrayList<String> indexList = new ArrayList<String>();
				indexList.add(column.index);
				indexList.add(column.indexTableName);
				dataFormat.put(key, indexList);
			}
		}
		return dataFormat;
	}
	
	/**
	 * Get the position of the column which has the index
	 * @return the position of the column which has the index
	 */
	public int[] getIndexPos(){
		generateIndexPos();
		return indexPos;
	}
	
	/**
	 * Get the index position at the specific position in the indexPos
	 * @param index the index of the index position
	 * @return the index position at the specific position in the indexPos
	 */
	public int getIndexPos(int index){
		generateIndexPos();
		return indexPos[index];
	}
	
	/**
	 * Get table name
	 * @return the table name
	 */
	public String getTableName(){
		return name;
	}
	
	/**
	 * Get the separator which splits the data set 
	 * @return the separator
	 */
	public byte getSeparator(){
		return separator;
	}
	
	/**
	 * Get the ColumnInfo list
	 * @return the ColumnInfo list
	 */
	public List<ColumnInfo> getColumnInfo(){
		return columns;
	}
	
	/**
	 * Get the ColumnInfo at the specific position in the ColumnInfo list
	 * @param pos the index of the position
	 * @return the ColumnInfo at the specific position in the ColumnInfo list
	 */
	public ColumnInfo getColumnInfo(int pos){
		return columns.get(pos);
	}
	
	/**
	 * Get the ColumnInfo by its column name "family:qualifier"
	 * @param colName the column name "family:qualifier"
	 * @return if it exist return it, otherwise return null
	 */
	public ColumnInfo getColumnInfoByName(String colName){
		String[] name = colName.split(":");
		for(ColumnInfo col : columns){
			if(name[0].equals(Bytes.toString(col.getFamily())) && name[1].equals(Bytes.toString(col.getQualifier())))
				return col;
		}
		return null;
	}
	
	public void removeKey() {
		for(Iterator<ColumnInfo> it=columns.iterator();it.hasNext();){
			ColumnInfo col = it.next();
			if(col.isKey())
				it.remove();
		}
		generatePos();
	}
	
	public int getKeyPos() {
		for(ColumnInfo col : columns){
			if(col.isKey())
				return col.getPos();
		}
		return -1;
	}
	
	public int getTimestampPos() {
		for(ColumnInfo col : columns){
			if(col.isTimestamp())
				return col.getPos();
		}
		return -1;
	}
	
	/**
	 * The ColumnInfo describe a column with its name, type and index
	 * @author gu
	 *
	 */
	public class ColumnInfo{
		private byte[] family;
		private byte[] qualifier;
		private String index;
		private String indexTableName;
		private int pos;
		
		public byte[] bytes;
		private DataType dataType;
		
		/**
		 * Construct the ColumnInfo with column name and index name, the type is the default value "String"
		 * @param familyName the family name of the column
		 * @param qualifierName the qualifier name of the column
		 * @param indexName the index type you used, includes "CCIDNEX"
		 * @param indexTableName the index table name
		 */
		public ColumnInfo(String familyName, String qualifierName, String indexName, String indexTableName){
			this(familyName, qualifierName, indexName, indexTableName, DataType.STRING.toString());
		}

		/**
		 * Construct the ColumnInfo with column name, index name and type
		* @param familyName the family name of the column
		 * @param qualifierName the qualifier name of the column
		 * @param indexName the index type you used, includes "CCIDNEX"
		 * @param indexTableName the index table name
		 * @param DataType the column type you used, include "String","Integer","Long","Double"
		 */
		public ColumnInfo(String familyName, String qualifierName, String indexName, String indexTableName, String DataType){
			this.family = Bytes.toBytes(familyName);
			this.qualifier = (qualifierName==null ? null : Bytes.toBytes(qualifierName));
			this.index = indexName==null?"null":indexName;
			this.indexTableName = indexTableName;
			this.dataType = (DataType)Enum.valueOf(DataType.class, DataType.toUpperCase());
			this.pos = columns.size();
			
			if(this.qualifier != null){
				int length = 2*Bytes.SIZEOF_BYTE + family.length + qualifier.length + Bytes.SIZEOF_LONG;
				bytes = new byte[length];
				int pos=0;
				pos = Bytes.putByte(bytes, pos, (byte)(family.length & 0x0000ff));
				pos = Bytes.putBytes(bytes, pos, family, 0, family.length);
				pos = Bytes.putBytes(bytes, pos, qualifier, 0, qualifier.length);
				pos = Bytes.putLong(bytes, pos, System.currentTimeMillis());
				pos = Bytes.putByte(bytes, pos, Type.Put.getCode());
			}
		}
		
		/**
		 * Return the column family
		 * @return the column family
		 */
		public byte[] getFamily(){
			return family;
		}
		
		/**
		 * Return the column qualifier
		 * @return the column qualifier
		 */
		public byte[] getQualifier(){
			return qualifier;
		}
		
		/**
		 * Return the index type of this column
		 * @return the index type
		 */
		public String getIndex(){
			return index;
		}
		
		/**
		 * Return the index table name of this column
		 * @return the index table name
		 */
		public String getIndexTableName(){
			return indexTableName;
		}
		
		
		/**
		 * Return the data type of this column
		 * @return the data type of this column
		 */
		public DataType getDataType(){
			return dataType;
		}
		
		public void setDataType(String type){
			dataType = (DataType)Enum.valueOf(DataType.class, type);
		}
		
		public int getPos(){
			return pos;
		}
		
		public void setPos(int pos){
			this.pos = pos;
		}
		
		public boolean isKey() {
			if(ImportConstants.KEY.equals(Bytes.toStringBinary(this.family)) && this.qualifier == null)
				return true;
			else
				return false;
		}
		
		public boolean isTimestamp() {
			if(ImportConstants.TIMESTAMP.equals(Bytes.toStringBinary(this.family)) && this.qualifier == null)
				return true;
			else
				return false;
		}
		
		/**
		 * Add index info into a exist ColumnInfo
		 * @param index index type
		 * @param indexTableName index table name
		 */
		public void addIndex(String index, String indexTableName){
			this.index = index;
			this.indexTableName = indexTableName;
		}
	}
	
	/**
	 * Used for test
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String args[]) throws Exception{
		//System.out.print((byte)'\t');
		
//		TableInfo info = TableInfo.createFromXML("/home/gu/workspace/MRImport-old/conf/table.xml");
//		TableInfo info = new TableInfo("test1,44,f:1:STRING:null:null,f2:2:string,f:2:LONG:null:null,f:3:LONG:null:null,f:4:INT:null:null,f:5:INT:null:null,f2:1:int,f3:1:long");
//		TableInfo info = new TableInfo("test,comma,name:1:String,name:2:String,name:3:String,name:4:String,name:5:String");
//		for(ColumnInfo col:info.getColumnInfo())
//			System.out.println(col.getPos());
//		ColumnInfo col = info.getColumnInfo(1);
//		System.out.println(info.toSimpleString());
//		byte[] row = Bytes.toBytes("row");
//		byte[] line = Bytes.toBytes("1000");
//		byte[] bytes = BulkLoadUtil.createKVByte(row, col.bytes, col.getDataType(), line, 0, line.length);
//		KeyValue kv = new KeyValue(bytes);
//		System.out.println(Bytes.toStringBinary(kv.getValue()));
		
//		IndexAdmin indexadmin=new IndexAdmin(HBaseConfiguration.create());
//		int n = Integer.valueOf(args[0]);
//		int m = Integer.valueOf(args[1]);
//		IndexType type = null;
//		switch(n){
//		case 1:type=IndexType.CCINDEX;break;
//		case 2:type=IndexType.SECONDARYINDEX;break;
//		case 3:type=IndexType.IMPSECONDARYINDEX;break;
//		case 4:indexadmin.disableTable(Bytes.toBytes("test"));indexadmin.deleteTable(Bytes.toBytes("test"));System.exit(0);
//		}
//		HTableDescriptor desc=new HTableDescriptor("test");
//		desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f")));
//		IndexSpecification[] index=new IndexSpecification[1];
//		index[0]=new IndexSpecification(Bytes.toBytes("f:2"),type);
//		if(n == 3){
//			index[0].addAdditionColumn(Bytes.toBytes("f"), Bytes.toBytes("1"));
//			index[0].addAdditionColumn(Bytes.toBytes("f"), Bytes.toBytes("3"));
//		}
//		if(m==1){
//			indexadmin.disableTable("test");
//			indexadmin.addIndexes(Bytes.toBytes("test"), index);
//			indexadmin.enableTable("test");
//		}
//		else if (m==0){
//			IndexTableDescriptor indexDesc=new IndexTableDescriptor(desc,index);
//			indexadmin.createTable(indexDesc);
//		}
		
//		TableInfo table = new TableInfo("test,44,f:1:String:null:null,f:2:String:CCINDEX:test-f_2,f:3:String:null:null,f:4:String:null:null,f:5:String:null:null");
//		TableInfo.ColumnInfo info = table.getColumnInfo(table.getIndexPos(1-1));
//		System.out.println(info.getIndexTableName());
//		
//		byte[] htdBytes = Bytes.toBytesBinary("\\x00\\x00\\x00\\x05\\x04test\\x00\\x00\\x00\\x00\\x00\\x05\\x00\\x00\\x00\\x08BASE_KEY\\x00\\x00\\x00\\x08BASE_KEY\\x00\\x00\\x00\\x07IS_ROOT\\x00\\x00\\x00\\x05false\\x00\\x00\\x00\\x06KEYGEN\\x00\\x00\\x00;ict.ocrabase.main.java.client.index.SimpleIndexKeyGenerator\\x00\\x00\\x00\\x07IS_META\\x00\\x00\\x00\\x05false\\x00\\x00\\x00\\x09INDEX_KEY\\x00\\x00\\x00\\x13\\x00\\x00\\x00\\x01\\x01\\x04test\\x03f_2\\x03f:2\\x00\\x00\\x00\\x00\\x01\\x08\\x01f\\x00\\x00\\x00\\x08\\x00\\x00\\x00\\x0BBLOOMFILTER\\x00\\x00\\x00\\x04NONE\\x00\\x00\\x00\\x11REPLICATION_SCOPE\\x00\\x00\\x00\\x010\\x00\\x00\\x00\\x08VERSIONS\\x00\\x00\\x00\\x013\\x00\\x00\\x00\\x0BCOMPRESSION\\x00\\x00\\x00\\x04NONE\\x00\\x00\\x00\\x03TTL\\x00\\x00\\x00\\x0A2147483647\\x00\\x00\\x00\\x09BLOCKSIZE\\x00\\x00\\x00\\x0565536\\x00\\x00\\x00\\x09IN_MEMORY\\x00\\x00\\x00\\x05false\\x00\\x00\\x00\\x0ABLOCKCACHE\\x00\\x00\\x00\\x04true");
//		HTableDescriptor htd = new HTableDescriptor();
//		
//		
//		htd.readFields(new DataInputStream(new ByteArrayInputStream(htdBytes)));
//		Configuration config = HBaseConfiguration.create();
//		config.addResource(new Path("file:///home/gu/hadoop-hbase/ictbase/ICTBase/conf/hbase-site.xml"));
//		config.reloadConfiguration();
//		HBaseAdmin admin = new HBaseAdmin(config);
//		HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes("test"));
//		System.out.println(htd.toString());
//		System.out.println(htd.getValue("COMPRESSION"));
//		IndexTableDescriptor itd = new IndexTableDescriptor(htd);
//		System.out.println(itd.getIndexSpecification(Bytes.toBytes("f:2")).getIndexType().getName());
//		info.deleteFamily("f2");
//		System.out.println(info.toSimpleString());
		TableInfo tab = new TableInfo("table,tab,KEY,CF1:col1:int,CF2:col6:string,TIMESTAMP");
//		tab.removeKey();
//		System.out.println(tab.getTimestampPos());
	}

	
}