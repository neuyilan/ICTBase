package ict.ictbase.commons.global;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GlobalHTableGetByIndex extends GlobalHTableWithIndexesDriver {

	int policyReadIndex;
	public static final int PLY_FASTREAD = 0;
	public static final int PLY_READCHECK = 1;

	public GlobalHTableGetByIndex(Configuration conf, byte[] tableName)
			throws IOException {
		super(conf, tableName);
		configPolicy(PLY_FASTREAD);
	}

	public void configPolicy(int p) {
		policyReadIndex = p;
	}

	public int getIndexingPolicy() {
		return policyReadIndex;
	}

//	public List<byte[]> getByIndex(byte[] columnFamily, byte[] columnName,
//			byte[] value) throws IOException {
//		List<byte[]> rawResults = readIndexOnly(columnFamily, columnName, value);
//		List<byte[]> datakeyToDels = new ArrayList<byte[]>();
//		if (policyReadIndex == PLY_READCHECK) {
//			if (rawResults != null) {
//				for (byte[] dataRowkey : rawResults) {
//					byte[] valueFromBase = readBase(dataRowkey, columnFamily,
//							columnName);
//					if (!Bytes.equals(valueFromBase, value)) {
//						datakeyToDels.add(dataRowkey);
//					}
//				}
//				rawResults.removeAll(datakeyToDels);
//				for (byte[] datakeyToDel : datakeyToDels) {
//					deleteFromIndex(columnFamily, columnName, value,
//							datakeyToDel);
//				}
//			}
//		}
//		return rawResults;
//	}
	
	public List<String> getByIndexData(byte[] columnFamily, byte[] columnName,
			byte[] value) throws IOException {
		List<String> res = internalGetByIndexByRange(columnFamily,
				columnName, value, null);
		return res;
	}
	
	

	private byte[] readBase(byte[] dataRowkey, byte[] columnFamily,
			byte[] columnName) throws IOException {
		Get g = new Get(dataRowkey);
		g.addColumn(columnFamily, columnName);
		Result r = this.get(g);
		assert r.rawCells().length == 1;
		Cell cell = r.rawCells()[0];
		return CellUtil.cloneValue(cell);
	}

//	private List<byte[]> readIndexOnly(byte[] columnFamily, byte[] columnName,
//			byte[] value) throws IOException {
//		assert value != null;
//		Map<byte[], List<byte[]>> res = internalGetByIndexByRange(columnFamily,
//				columnName, value, null);
//		if (res == null || res.size() == 0) {
//			return null;
//		} else {
//			List<byte[]> toRet = new ArrayList<byte[]>();
//			for (Map.Entry<byte[], List<byte[]>> e : res.entrySet()) {
//				List<byte[]> keys = e.getValue();
//				for (byte[] key : keys) {
//					toRet.add(key);
//				}
//			}
//			return toRet;
//		}
//	}
//
//	public Map<byte[], List<byte[]>> getByIndexByRange(byte[] columnFamily,
//			byte[] columnName, byte[] valueStart, byte[] valueEnd)
//			throws IOException {
//		assert valueStart != null;
//		assert valueEnd != null;
//		assert Bytes.toString(valueStart).compareTo(Bytes.toString(valueEnd)) < 0; 
//		return internalGetByIndexByRange(columnFamily, columnName, valueStart,
//				Bytes.toBytes(Bytes.toString(valueEnd) + "0"));
//	}
	
	
	
}
