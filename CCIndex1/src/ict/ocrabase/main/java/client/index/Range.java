package ict.ocrabase.main.java.client.index;

import ict.ocrabase.main.java.regionserver.DataType;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Structure to store one certain column's query range.
 * 
 * @author liujia, modified by wanhao
 * 
 */
public class Range {
	private byte[] baseTable = null;
	private byte[] column = null;
	private byte[] startValue = null;
	private byte[] endValue = null;
	private CompareOp startType;
	private CompareOp endType;
	private List<Pair<CompareOp,List<byte[]>>> additionalOp =  new ArrayList<Pair<CompareOp,List<byte[]>>>();
	
	private long startTs = -1;
	private long endTs = -1;
	private boolean latestVersionOnly = false;
	private boolean filterIfMissing = true;
	private byte[][] baseColumns = null;

	public byte[] getBaseTable(){
		return this.baseTable;
	}
	
	public void setBaseTable(byte[] baseTable){
		this.baseTable = baseTable;
	}

	public List<Pair<CompareOp,List<byte[]>>> getAdditionalOp(){
		return this.additionalOp;
	}
	
	public void addAdditionalOp(Pair<CompareOp,List<byte[]>> pair){
		this.additionalOp.add(pair);
	}
	
	public byte[][] getBaseColumns() {
		return baseColumns;
	}

	public void setBaseColumns(byte[][] baseColumn) {
		this.baseColumns = baseColumn;
	}

	public boolean isLatestVersionOnly() {
		return latestVersionOnly;
	}

	public void setLatestVersionOnly(boolean latestVersionOnly) {
		this.latestVersionOnly = latestVersionOnly;
	}

	public boolean isFilterIfMissing() {
		return filterIfMissing;
	}

	public void setFilterIfMissing(boolean filterIfMissing) {
		this.filterIfMissing = filterIfMissing;
	}

	// valueType:1 stand for string type,2 stand for integer type,3 stand for
	// float type,4 stand for double
	private DataType valueType = DataType.STRING;

	// kept for ScanTree, will be deleted later
	public Range() {

	}

	/**
	 * Construct a Range whose start value and end value are null.
	 * 
	 * @param column
	 */
	public Range(byte[] baseTable,byte[] column) {
		this.baseTable=baseTable;
		setColumn(column);
		this.startValue = null;
		this.endValue = null;
		this.startType = CompareOp.GREATER_OR_EQUAL;
		this.endType = CompareOp.LESS_OR_EQUAL;
	}

	/**
	 * Construct a Range with given parameters.
	 * 
	 * @param column
	 * @param startValue
	 * @param startType
	 *            - can only be EQUAL, GREATER or GREATER_OR_EQUAL
	 * @param endValue
	 * @param endType
	 *            - can only be LESS or LESS_OR_EQUAL
	 */
	public Range(byte[] baseTable,byte[] column, byte[] startValue, CompareOp startType,
			byte[] endValue, CompareOp endType) {
		this.baseTable=baseTable;
		setColumn(column);
		this.startValue = startValue;
		setStartType(startType);
		this.endValue = endValue;
		setEndType(endType);
	}

	public Range(byte[] baseTable,byte[] column, byte[] startValue, CompareOp startType,
			byte[] endValue, CompareOp endType, long startTs, long endTs) {
		this.baseTable=baseTable;
		setColumn(column);
		this.startValue = startValue;
		setStartType(startType);
		this.endValue = endValue;
		setEndType(endType);
		this.startTs = startTs;
		this.endTs = endTs;
	}

	public long getStartTs() {
		return startTs;
	}

	public void setStartTs(long startTs) {
		this.startTs = startTs;
	}

	public long getEndTs() {
		return endTs;
	}

	public void setEndTs(long endTs) {
		this.endTs = endTs;
	}

	public CompareOp getStartType() {
		return this.startType;
	}

	/**
	 * Set start type for start value.
	 * 
	 * @param startType
	 *            - can only be EQUAL, GREATER or GREATER_OR_EQUAL
	 */
	public void setStartType(CompareOp startType) {
		if (startType == CompareOp.EQUAL || startType == CompareOp.GREATER
				|| startType == CompareOp.GREATER_OR_EQUAL) {
			this.startType = startType;
		} else {
			throw new IllegalArgumentException("Illegal start type: "
					+ startType.toString());
		}
	}

	public CompareOp getEndType() {
		return this.endType;
	}

	/**
	 * Set end type for end value.
	 * 
	 * @param endType
	 *            - can only be LESS or LESS_OR_EQUAL
	 */
	public void setEndType(CompareOp endType) {
		if (endType == CompareOp.LESS || endType == CompareOp.LESS_OR_EQUAL) {
			this.endType = endType;
		} else {
			throw new IllegalArgumentException("Illegal end type: "
					+ endType.toString());
		}
	}

	public byte[] getColumn() {
		return column;
	}

	/**
	 * Set column name for the Range.
	 * 
	 * @param column
	 */
	public void setColumn(byte[] column) {
		if (column == null || column.length == 0) {
			throw new IllegalArgumentException("Column name is empty!");
		}
		this.column = column;
	}

	public byte[] getStartValue() {
		return startValue;
	}

	/**
	 * Set start value for the Range.
	 * 
	 * @param startValue
	 */
	public void setStartValue(byte[] startValue) {
		this.startValue = startValue;
	}

	public byte[] getEndValue() {
		return endValue;
	}

	/**
	 * Set value type for the startValue and endValue.
	 * 
	 * @param valueType
	 */
	public void setValueType(DataType valueType) {
		this.valueType = valueType;
	}

	public DataType getValueType() {
		return valueType;
	}

	/**
	 * Set end value for the Range.
	 * 
	 * @param endValue
	 */
	public void setEndValue(byte[] endValue) {
		this.endValue = endValue;
	}

	public Range copyRange() {
		Range ret = new Range();
		ret.column = this.column.clone();
		ret.endType = this.endType;
		if (this.endValue != null)
			ret.endValue = this.endValue.clone();
		ret.startType = this.startType;
		if (this.startValue != null)
			ret.startValue = this.startValue.clone();
		ret.valueType = this.valueType;
		
		for(int i=0;i<this.additionalOp.size();i++){
			Pair<CompareOp,List<byte[]>> pair = new Pair<CompareOp,List<byte[]>>();
			pair.setFirst(((Pair<CompareOp,List<byte[]>>)this.additionalOp.get(i)).getFirst());
			pair.setSecond(((Pair<CompareOp,List<byte[]>>)this.additionalOp.get(i)).getSecond());
			ret.addAdditionalOp(pair);
		}
		ret.setStartTs(startTs);
		ret.setEndTs(endTs);
		ret.setLatestVersionOnly(latestVersionOnly);
		ret.setFilterIfMissing(filterIfMissing);
		if(this.baseColumns!=null){
			byte[][] byteT = new byte[this.baseColumns.length][];
			for(int i=0;i<this.baseColumns.length;i++){
				byteT[i]= new byte[this.baseColumns[i].length];
				for(int j=0;j<this.baseColumns[i].length;j++){
					byteT[i][j]=this.baseColumns[i][j];
				}
			}
			ret.setBaseColumns(byteT);
		}
		return ret;
	}

	public Range getOpsite() {
		Range ran = new Range();
		ran.column = this.column;
		ran.valueType = this.valueType;
		ran.startValue = this.endValue;
		ran.endValue = this.startValue;
		ran.setStartTs(startTs);
		ran.setEndTs(endTs);
		ran.setLatestVersionOnly(latestVersionOnly);
		ran.setFilterIfMissing(filterIfMissing);
		
		if (ran.valueType == DataType.BOOLEAN) {
			if (Bytes.toString(this.startValue).equals("true")) {
				ran.startValue = Bytes.toBytes("false");
			} else if (Bytes.toString(this.startValue).equals("false")) {
				ran.startValue = Bytes.toBytes("true");
			}
			ran.startType = CompareOp.EQUAL;
			return ran;
		}
		if (this.startValue != null && this.startValue.length != 0) {
			switch (this.startType) {
			case EQUAL:
				ran.startType = CompareOp.GREATER;
				ran.startValue = this.startValue;
				ran.endValue = this.startValue;
				ran.endType = CompareOp.LESS;
				break;
			case GREATER_OR_EQUAL:
				ran.endType = CompareOp.LESS;
				break;
			case GREATER:
				ran.endType = CompareOp.LESS_OR_EQUAL;
				break;
			default:
				break;
			}
		}
		if (this.endValue != null && this.endValue.length != 0) {
			switch (this.endType) {
			case LESS:
				ran.startType = CompareOp.GREATER_OR_EQUAL;
				break;
			case LESS_OR_EQUAL:
				ran.startType = CompareOp.GREATER;
				break;
			default:
				break;
			}
		}
		
    /*
     * for(int i=0;i<this.additionalOp.size();i++){ Pair<CompareOp,List<byte[]>> pair = new
     * Pair<CompareOp,List<byte[]>>();
     * if(((Pair<CompareOp,List<byte[]>>)this.additionalOp.get(i)).getFirst()==CompareOp.NOT_LIKE){
     * pair.setFirst(CompareOp.LIKE); }else
     * if(((Pair<CompareOp,List<byte[]>>)this.additionalOp.get(i)).getFirst()==CompareOp.LIKE){
     * pair.setFirst(CompareOp.NOT_LIKE); }
     * pair.setSecond(((Pair<CompareOp,List<byte[]>>)this.additionalOp.get(i)).getSecond());
     * ran.addAdditionalOp(pair); }
     */

		return ran;
	}

	public String getContain() {
		StringBuilder sb = new StringBuilder();
		if(baseTable!=null){
			sb.append("baseTable:"+Bytes.toString(this.baseTable)+"\t");
		}
		if(column!=null){
			sb.append("column:"+Bytes.toString(this.column)+"\t");
		}
		if (startValue != null && startValue.length != 0 && startType != null) {
			sb.append(Bytes.toString(column));

			switch (startType) {
			case EQUAL:
				sb.append("=");
				break;
			case GREATER_OR_EQUAL:
				sb.append(">=");
				break;
			case GREATER:
				sb.append(">");
				break;
			case NOT_EQUAL:
				sb.append("!=");
				break;
			default:
				break;
			}

			sb.append(Bytes.toString(startValue));
		}
		if (startValue != null && startValue.length != 0 && endValue != null
				&& endValue.length != 0 && endType != null && startType != null) {
			int mark = 0;
			switch (valueType) {
			case STRING:
				if (Bytes.compareTo(endValue, startValue) <= 0)
					mark = 1;
				break;
			case INT:
				if (Integer.parseInt(Bytes.toString(endValue)) <= Integer
						.parseInt(Bytes.toString(startValue)))
					mark = 1;
				break;
			case TINYINT:
				if (Integer.parseInt(Bytes.toString(endValue)) <= Integer
						.parseInt(Bytes.toString(startValue)))
					mark = 1;
				break;
			case MEDIUMINT:
				if (Integer.parseInt(Bytes.toString(endValue)) <= Integer
						.parseInt(Bytes.toString(startValue)))
					mark = 1;
				break;
			case LONG:
				if (Long.parseLong(Bytes.toString(endValue)) <= Long
						.parseLong(Bytes.toString(startValue)))
					mark = 1;
				break;
			case DOUBLE:
				if (Double.parseDouble(Bytes.toString(endValue)) <= Double
						.parseDouble(Bytes.toString(startValue)))
					mark = 1;
				break;
			default:
				break;
			}

			if (mark == 1)
				sb.append(" or ");
			else
				sb.append(" and ");
		}

		if (endValue != null && endValue.length != 0 && endType != null) {
			sb.append(Bytes.toString(column));

			switch (endType) {
			case LESS_OR_EQUAL:
				sb.append("<=");
				break;
			case LESS:
				sb.append("<");
				break;
			default:
				break;
			}
			sb.append(Bytes.toString(endValue));
		}
		sb.append(",startTs:"+this.startTs);
		sb.append(",endTs:"+this.endTs);
		sb.append(",filterIfMissing:"+filterIfMissing);
		sb.append(",latestVersionOnly:"+latestVersionOnly);
		if(this.baseColumns!=null){
			sb.append(",baseColumns:");
			for(byte[] b:this.baseColumns){
				sb.append(Bytes.toString(b)+",");
			}
		}
		sb.append(",additionalOp");
		if(this.additionalOp!=null){
			for(Pair<CompareOp,List<byte[]>> p:(ArrayList<Pair<CompareOp,List<byte[]>>>)this.additionalOp){
				sb.append(p.getFirst());
				for(byte[] b:p.getSecond()){
					sb.append(":"+Bytes.toString(b));
				}
					
				
			}
		}
		return sb.toString();
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		if(baseTable!=null){
			sb.append("baseTable:"+Bytes.toString(this.baseTable)+"\t");
		}
		if(column!=null){
			sb.append("column:"+Bytes.toString(this.column)+"\t");
		}
		if (startValue != null && startValue.length != 0 && startType != null) {
			sb.append(Bytes.toString(column));

			switch (startType) {
			case EQUAL:
				sb.append("=");
				break;
			case GREATER_OR_EQUAL:
				sb.append(">=");
				break;
			case GREATER:
				sb.append(">");
				break;
			case NOT_EQUAL:
				sb.append("!=");
				break;
			default:
				break;
			}

			switch (valueType) {
			case STRING:
				sb.append(Bytes.toString(startValue));
				break;
			case INT:
				sb.append(Bytes.toInt(startValue));
				break;
			case LONG:
				sb.append(Bytes.toLong(startValue));
				break;
			case DOUBLE:
				sb.append(Bytes.toDouble(startValue));
				break;
			case BOOLEAN:
				sb.append(Bytes.toBoolean(startValue));
				break;

			default:
				break;
			}

		}
		if (startValue != null && startValue.length != 0 && endValue != null
				&& endValue.length != 0 && endType != null && startType != null) {
			int mark = 0;
			switch (valueType) {
			case STRING:
				if (Bytes.compareTo(endValue, startValue) <= 0)
					mark = 1;
				break;
			case INT:
				if (Bytes.toInt(endValue) <= Bytes.toInt(startValue))
					mark = 1;
				break;

			case LONG:
				if (Bytes.toLong(endValue) <= Bytes.toLong(startValue))
					mark = 1;
				break;
			case DOUBLE:
				if (Bytes.toDouble(endValue) <= Bytes.toDouble(startValue))
					mark = 1;
				break;
			default:
				break;
			}

			if (mark == 1)
				sb.append(" or ");
			else
				sb.append(" and ");
		}

		if (endValue != null && endValue.length != 0 && endType != null) {
			sb.append(Bytes.toString(column));

			switch (endType) {
			case LESS_OR_EQUAL:
				sb.append("<=");
				break;
			case LESS:
				sb.append("<");
				break;
			default:
				break;
			}

			switch (valueType) {
			case STRING:
				sb.append(Bytes.toString(endValue));
				break;
			case INT:
				sb.append(Bytes.toInt(endValue));
				break;
			case LONG:
				sb.append(Bytes.toLong(endValue));
				break;
			case DOUBLE:
				sb.append(Bytes.toDouble(endValue));
				break;

			default:
				break;
			}
		}
		String additional = "";
		if(this.additionalOp!=null){
			for(int i=0;i<this.additionalOp.size();i++){
				Pair<CompareOp,List<byte[]>> p = (Pair<CompareOp,List<byte[]>>)this.additionalOp.get(i);
				additional += p.getFirst();
				for(byte[] b:p.getSecond()){
					additional += ":"+Bytes.toString(b);
				}
			}
		}
		return sb.toString() + ",startTs:" + startTs + ",endTs:" + endTs +",filterIfMissing:"+filterIfMissing+",latestVersionOnly:"+latestVersionOnly+",additional:"+ additional;
	}

}
