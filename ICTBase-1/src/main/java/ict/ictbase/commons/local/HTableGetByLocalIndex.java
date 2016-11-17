package ict.ictbase.commons.local;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

public class HTableGetByLocalIndex extends HTableWithLocalIndexesDriver {

	int policyReadIndex;
	public static final int PLY_FASTREAD = 0;
	public static final int PLY_READCHECK = 1;

	public HTableGetByLocalIndex(Configuration conf, byte[] tableName)
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

	private byte[] readBase(byte[] dataRowkey, byte[] columnFamily,
			byte[] columnName) throws IOException {
		Get g = new Get(dataRowkey);
		g.addColumn(columnFamily, columnName);
		Result r = this.get(g);
		assert r.rawCells().length == 1;
		Cell cell = r.rawCells()[0];
		return CellUtil.cloneValue(cell);
	}

}
