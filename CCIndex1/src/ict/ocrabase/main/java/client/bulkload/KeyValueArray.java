package ict.ocrabase.main.java.client.bulkload;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Implement a KeyValue array, it actually a KeyValue[]
 * @author gu
 *
 */
public class KeyValueArray implements Writable{
	private KeyValue[] values;

	public KeyValueArray(){
		this.values = null;
	}
	
	/**
	 * Use a KeyValue array to construct KeyValueArray
	 * @param kvs
	 */
	public KeyValueArray(KeyValue[] kvs) {
		this.values = kvs;
	}
	
	public KeyValueArray(Collection<KeyValue> c){
		this.values = c.toArray(new KeyValue[0]);
	}

	  /**
   * Convert and copy KeyValueArray to KeyValue[]
   * @return the KeyValue array
   * @throws CloneNotSupportedException
   */
  public KeyValue[] toArray() throws CloneNotSupportedException {
		KeyValue[] result = new KeyValue[values.length];
		for (int i = 0; i < values.length; i++) {
			result[i] = values[i].clone();
		}
		return result;
	}

	/**
	 * Set the KeyValue[] to KeyValueArray
	 * @param values 
	 */
	public void set(KeyValue[] values) {
		this.values = values;
	}

	/**
	 * Just convert the KeyValueArray to KeyValue[]
	 * @return
	 */
	public KeyValue[] get() {
		return values;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		values = new KeyValue[in.readInt()]; // construct values
		for (int i = 0; i < values.length; i++) {
			KeyValue value = new KeyValue();
      value.create(in); // read a value////////
			values[i] = value; // store it in values
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(values.length); // write values
		for (int i = 0; i < values.length; i++) {
      values[i].write(values[i], out);
		}
	}
	
	/**
	 * Compare with its row, family, qualifier and value
	 */
	@Override
	public boolean equals(Object right_object){
		if(right_object instanceof KeyValueArray){
			KeyValue[] kvs = ((KeyValueArray)right_object).get();
			if(values.length != kvs.length)
				return false;
			else{
				for(int i=0;i<values.length;i++){
					if(Bytes.BYTES_RAWCOMPARATOR.compare(kvs[i].getRow(), values[i].getRow()) != 0 ||
							Bytes.BYTES_RAWCOMPARATOR.compare(kvs[i].getFamily(), values[i].getFamily()) != 0 ||
							Bytes.BYTES_RAWCOMPARATOR.compare(kvs[i].getQualifier(), values[i].getQualifier()) != 0 || 
							Bytes.BYTES_RAWCOMPARATOR.compare(kvs[i].getValue(), values[i].getValue()) != 0)
						return false;
				}
			}
		}else
			return false;
		
		return true;
	}
	
	@Override
	public String toString(){
		String out = "{ ";
		for(KeyValue kv : values){
			out += kv.toString();
			out += "  ";
		}
		out += " }";
		return out;
	}

}
