package ict.ocrabase.main.java.client.bulkload;


import ict.ocrabase.main.java.regionserver.DataType;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * It contains some frequently used methods
 * @author gu
 *
 */
public class BulkLoadUtil {

	/**
	 * Get all the files in the given directory
	 * @param fs where is the dir in
	 * @param dir the directory to scan
	 * @return all the files
	 * @throws IOException
	 */
	public static List<Path> scanDirectory(FileSystem fs, Path dir) throws IOException {
		List<Path> files = new ArrayList<Path>();
		if(!fs.getFileStatus(dir).isDir())
			files.add(dir);
		else{
			FileStatus fileList[] = fs.listStatus(dir);
			int fileNum = fileList.length;
			for (int fileCount = 0; fileCount < fileNum; fileCount++) {
				if(fileList[fileCount].getPath().getName().matches("(_|\\\\.).*"))
					continue;
			
				if (fileList[fileCount].isDir())
					files.addAll(scanDirectory(fs,fileList[fileCount].getPath()));
				else if(fileList[fileCount].getLen() != 0)
					files.add(fileList[fileCount].getPath());
			}
		}
		return files;
	}
	
	/**
	 * Split the text by the separator
	 * @param separator the separator
	 * @param line the text line you want to split
	 * @param length the length of the text line
	 * @return
	 */
	public static Integer[] dataSplit(byte separator, byte[] line, int length) {
		ArrayList<Integer> split = new ArrayList<Integer>();
		split.add(-1);
		for(int i=0;i < length;i++){
			if(line[i] == separator)
				split.add(i);
		}
		split.add(length);
		return split.toArray(new Integer[0]);
	}
	
	/**
	 * Convert scan to a string, so that we can transfer the scan through configuration
	 * @param scan the scan you want to convert
	 * @return the converted string
	 * @throws IOException
	 */
	public static String convertScanToString(Scan scan) throws IOException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    DataOutputStream dos = new DataOutputStream(out);

    org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan protoscan =
        ProtobufUtil.toScan(scan);

    // scan.write(dos);
    return Base64.encodeBytes(protoscan.toByteArray());
	  }
	
	/**
	 * Create a KeyValue byte array, merge the function of createKVByte and convertToValueBytes
	 * @param keyBytes
	 * @param putBytes
	 * @param dataType
	 * @param value
	 * @param voffset
	 * @param vlength
	 * @return
	 */
	public static byte[] createKVByte(byte[] keyBytes, byte[] putBytes, DataType dataType, byte[] value, int voffset, int vlength){
		if(value==null){
			vlength = 0;
			value = new byte[0];
		}
		byte[] valueBytes = null;
		byte[] kvBytes = null;
		
		switch(dataType){
		case STRING:
			kvBytes = createKVByte(keyBytes, putBytes, value, voffset, vlength);
			break;
		case DOUBLE:
			valueBytes = Bytes.toBytes(Double.valueOf(Bytes.toString(value, voffset, vlength)));
			kvBytes = createKVByte(keyBytes, putBytes, valueBytes, 0, valueBytes.length);
			break;
		case LONG:
			valueBytes = Bytes.toBytes(Long.valueOf(Bytes.toString(value, voffset, vlength)));
			kvBytes = createKVByte(keyBytes, putBytes, valueBytes, 0, valueBytes.length);
			break;
		case INT:
			valueBytes = Bytes.toBytes(Integer.valueOf(Bytes.toString(value, voffset, vlength)));
			kvBytes = createKVByte(keyBytes, putBytes, valueBytes, 0, valueBytes.length);
			break;
		case BOOLEAN:
			valueBytes = Bytes.toBytes(Boolean.valueOf(Bytes.toString(value, voffset, vlength)));
			kvBytes = createKVByte(keyBytes, putBytes, valueBytes, 0, valueBytes.length);
			break;
    // case TINYINT:
    // valueBytes = Bytes.toTinyIntBytes(Integer.valueOf(Bytes.toString(value, voffset, vlength)));
    // kvBytes = createKVByte(keyBytes, putBytes, valueBytes, 0, valueBytes.length);
    // break;
		case SHORT:
			valueBytes = Bytes.toBytes(Short.valueOf(Bytes.toString(value, voffset, vlength)));
			kvBytes = createKVByte(keyBytes, putBytes, valueBytes, 0, valueBytes.length);
			break;
    // case MEDIUMINT:
    // valueBytes = Bytes.toMediumIntBytes(Integer.valueOf(Bytes.toString(value, voffset,
    // vlength)));
    // kvBytes = createKVByte(keyBytes, putBytes, valueBytes, 0, valueBytes.length);
    // break;
		}
		
		return kvBytes;
	}
	
	/**
	 * Create a KeyValue byte array
	 * @param keyBytes
	 * @param putBytes
	 * @param value
	 * @param voffset
	 * @param vlength
	 * @return
	 */
	public static byte[] createKVByte(byte[] keyBytes, byte[] putBytes, byte[] value,
			int voffset, int vlength) {
		int klength = Bytes.SIZEOF_SHORT + keyBytes.length + putBytes.length;
		byte[] bytes = new byte[Bytes.SIZEOF_INT*2+klength+vlength];
		int pos = 0;
		pos = Bytes.putInt(bytes, pos, klength);
		pos = Bytes.putInt(bytes, pos, vlength);
		pos = Bytes.putShort(bytes, pos, (short)(keyBytes.length & 0x0000ffff));
		pos = Bytes.putBytes(bytes, pos, keyBytes, 0, keyBytes.length);
		pos = Bytes.putBytes(bytes, pos, putBytes, 0, putBytes.length);
		if(vlength != 0)
			pos = Bytes.putBytes(bytes, pos, value, voffset, vlength);
		return bytes;
	}
	
	/**
	 * Create value bytes according to the data type
	 * @param dataType
	 * @param bytes
	 * @param offset
	 * @param length
	 * @return
	 */
	public static byte[] convertToValueBytes(DataType dataType, byte[] bytes, int offset, int length){
		if(bytes == null || length == 0)
			return new byte[0];
		else{
			byte[] valueBytes = null;
			switch(dataType){
			case STRING:
				valueBytes = new byte[length];
				System.arraycopy(bytes, offset, valueBytes, 0, length);
				break;
			case DOUBLE:
				valueBytes = Bytes.toBytes(Double.valueOf(Bytes.toString(bytes,offset, length)));
				break;
			case LONG:
				valueBytes = Bytes.toBytes(Long.valueOf(Bytes.toString(bytes,offset, length)));
				break;
			case INT:
				valueBytes = Bytes.toBytes(Integer.valueOf(Bytes.toString(bytes, offset, length)));
				break;
			case BOOLEAN:
				valueBytes = Bytes.toBytes(Boolean.valueOf(Bytes.toString(bytes, offset, length)));
				break;
      // case TINYINT:
      // valueBytes = Bytes.toTinyIntBytes(Integer.valueOf(Bytes.toString(bytes, offset, length)));
      // break;
			case SHORT:
				valueBytes = Bytes.toBytes(Short.valueOf(Bytes.toString(bytes, offset, length)));
				break;
      // case MEDIUMINT:
      // valueBytes = Bytes.toMediumIntBytes(Integer.valueOf(Bytes.toString(bytes, offset,
      // length)));
      // break;
			}
			return valueBytes;
		}
		
	}

}
