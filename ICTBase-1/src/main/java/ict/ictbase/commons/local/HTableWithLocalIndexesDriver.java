package ict.ictbase.commons.local;

import ict.ictbase.commons.MaterializeIndex;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class HTableWithLocalIndexesDriver extends HTable {
	protected static int errorIndex = 0;
	protected Map<String, HTable> indexTables = null;
	protected MaterializeIndex policyToMaterializeIndex = null;

	@SuppressWarnings("deprecation")
	public HTableWithLocalIndexesDriver(Configuration conf, byte[] tableName)
			throws IOException {
		super(conf, tableName);
		policyToMaterializeIndex = new MaterializeLocalIndexByCompositeRowkey(); // TOREMOVE
	}

	public void putToIndex(String regionStartKey,byte[] columnFamily, byte[] columnName,
			byte[] dataValue, byte[] dataKey) throws IOException {
		HTable indexTable = getIndexTable(columnFamily, columnName);
		policyToMaterializeIndex.putToIndex(indexTable,regionStartKey,columnFamily,columnName, dataValue, dataKey);
	}

	public HTable getIndexTable(byte[] columnFamily, byte[] columnName) {
		String dataTableName = Bytes.toString(this.getTableName());
		Connection con;
		HTable dataTable = null;
		try {
			con = ConnectionFactory.createConnection(this.getConfiguration());
			TableName tableName = TableName.valueOf(dataTableName);
			dataTable = (HTable) con.getTable(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dataTable;
	}
}
