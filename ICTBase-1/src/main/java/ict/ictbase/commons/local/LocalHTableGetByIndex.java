package ict.ictbase.commons.local;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

public class LocalHTableGetByIndex extends LocalHTableWithIndexesDriver {

	int policyReadIndex;
	public static final int PLY_FASTREAD = 0;
	public static final int PLY_READCHECK = 1;

	public LocalHTableGetByIndex(Configuration conf, byte[] tableName)
			throws IOException {
		super(conf, tableName);
		// default is baseline
		configPolicy(PLY_FASTREAD);
	}

	public void configPolicy(int p) {
		policyReadIndex = p;
	}

	public int getIndexingPolicy() {
		return policyReadIndex;
	}
	
	public List<String> getByIndex(byte[] columnFamily, byte[] columnName,
			byte[] value) throws IOException {
		if(isExistIndex(columnFamily, columnName)){
			System.out.println("HTableGetByLocalIndex********: true");
			return internalGetByIndexByRange(columnFamily,columnName,value,null);
		}else{
			System.out.println("HTableGetByLocalIndex********: false");
		}
		return null;
	}
}
