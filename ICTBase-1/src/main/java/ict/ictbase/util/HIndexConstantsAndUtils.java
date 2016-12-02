package ict.ictbase.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Connection;

public class HIndexConstantsAndUtils {
	public static final Log LOG = LogFactory
			.getLog(HIndexConstantsAndUtils.class);
	// TODO refactor: to match with HTableWithIndexesDriver.java
	// TODO refactor: to match with tthbase.util.UpdateCoprocessor.java
	public static final String INDEX_INDICATOR = "secondaryIndex$";
	public static final String INDEX_DELIMITOR = "|";

	// TODO make it a static function in commons.jar.
	public static byte[] generateIndexTableName(byte[] dataTableName,
			byte[] columnFamily, byte[] columnName) {
		return Bytes.toBytes(Bytes.toString(dataTableName) + '_'
				+ Bytes.toString(columnFamily) + '_'
				+ Bytes.toString(columnName));
	}

	// TODO copied from and refined based on UpdateCoprocessor.java
	public static void updateCoprocessor(Configuration conf, byte[] tableName,
			int indexOfCoprocessor, boolean ifUpdateorRemove,
			String coprocessorLoc, String coprocessorClassname)
			throws IOException {
		String rawAttributeName = "coprocessor$";
		String value = coprocessorLoc + "|" /* Not index delimitor */
				+ coprocessorClassname + "|1001|arg1=1,arg2=2";
		updateTableAttribute(conf, tableName, rawAttributeName,
				indexOfCoprocessor, ifUpdateorRemove, value);
	}
	

	public static void updateIndexIndicator(Configuration conf,
			byte[] tableName, int indexOfIndexIndicator,
			boolean ifUpdateorRemove, String indexedCF, String indexedColumn)
			throws IOException {
		String rawAttributeName = INDEX_INDICATOR;
		String value = indexedCF + INDEX_DELIMITOR + indexedColumn;
		updateTableAttribute(conf, tableName, rawAttributeName,
				indexOfIndexIndicator, ifUpdateorRemove, value);
	}

	/**
	 * @param rawAttributeName
	 *            is the attribute name viewed by applications, it allows
	 *            multiple values. For example, secondaryIndex in
	 *            secondaryIndex$1 and coprocessor in corpcessor$2
	 * @param indexOfAttribute
	 *            is of the same raw attribute name, for example 2 in
	 *            secondary$2
	 */
	static void updateTableAttribute(Configuration conf, byte[] tableName,
			String rawAttributeName, int indexOfAttribute,
			boolean ifUpdateorRemove, String value) throws IOException {
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(tableName);
		HTableDescriptor desc = admin.getTableDescriptor(tn);
		admin.disableTable(tn);
		LOG.debug("TTDEBUG: disable table " + Bytes.toString(tableName));
		String coprocessorKey = rawAttributeName + indexOfAttribute;
		if (!ifUpdateorRemove) {
			desc.remove(Bytes.toBytes(coprocessorKey));
		} else {
			desc.setValue(coprocessorKey, value);
		}
		admin.modifyTable(tn, desc);
		LOG.debug("TTDEBUG: modify table " + Bytes.toString(tableName));
		admin.enableTable(tn);
		LOG.debug("TTDEBUG: enable table " + Bytes.toString(tableName));
		HTableDescriptor descNew = admin.getTableDescriptor(tn);
		// modify table is asynchronous, has to loop over to check
		while (!desc.equals(descNew)) {
			LOG.error("TTDEBUG: waiting for descriptor to change: from "
					+ descNew + " to " + desc);
			try {
				Thread.sleep(500);
			} catch (InterruptedException ex) {
			}
			descNew = admin.getTableDescriptor(tn);
		}
	}

	public static void createAndConfigBaseTable(Configuration conf,
			byte[] tableName, byte[] columnFamily, String[] indexedColumnNames)
			throws IOException {
		// create a table with column familiy columnFamily

		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(tableName);
		HTableDescriptor desc = new HTableDescriptor(tn);
		// specify indexable columns.
		for (int i = 0; i < indexedColumnNames.length; i++) {
			desc.setValue(INDEX_INDICATOR + (i + 1),
					Bytes.toString(columnFamily) + INDEX_DELIMITOR
							+ indexedColumnNames[i]);
		}
		HColumnDescriptor descColFamily = new HColumnDescriptor(columnFamily);
		// configure to set KEEP_DELETED_CELLS => 'true'
		descColFamily.setKeepDeletedCells(KeepDeletedCells.TRUE);
		descColFamily.setTimeToLive(HConstants.FOREVER);
//		descColFamily.setMaxVersions(Integer.MAX_VALUE);
		descColFamily.setMaxVersions(1);
		desc.addFamily(descColFamily);
		admin.createTable(desc);
	}
	
	public static void createAndConfigAndSplitBaseTable(Configuration conf,
			byte[] tableName, byte[] columnFamily, byte[] indexTableColumnFamily,String[] indexedColumnNames, String startKeyStr,String endKeyStr,int numberOfRegions)
			throws IOException {
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(tableName);
		HTableDescriptor desc = new HTableDescriptor(tn);
		// specify indexable columns.
		for (int i = 0; i < indexedColumnNames.length; i++) {
			desc.setValue(INDEX_INDICATOR + (i + 1),
					Bytes.toString(columnFamily) + INDEX_DELIMITOR
							+ indexedColumnNames[i]);
		}
		HColumnDescriptor descColFamily = new HColumnDescriptor(columnFamily);
		descColFamily.setKeepDeletedCells(KeepDeletedCells.TRUE);
		descColFamily.setTimeToLive(HConstants.FOREVER);
		descColFamily.setMaxVersions(Integer.MAX_VALUE);
		desc.addFamily(descColFamily);
		
		HColumnDescriptor indexColFamily = new HColumnDescriptor(indexTableColumnFamily);
		indexColFamily.setKeepDeletedCells(KeepDeletedCells.TRUE);
		indexColFamily.setTimeToLive(HConstants.FOREVER);
		indexColFamily.setMaxVersions(Integer.MAX_VALUE);
		desc.addFamily(indexColFamily);
		
		byte [] startKey = Bytes.toBytes(startKeyStr);
		byte [] endKey = Bytes.toBytes(endKeyStr);
		admin.createTable(desc,startKey,endKey,numberOfRegions);
	}
	
	

	public static void createAndConfigIndexTable(Configuration conf,
			byte[] tableName, byte[] columnFamily) throws IOException {
		// create a table with column familiy columnFamily
		TableName tn = TableName.valueOf(tableName);
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		HTableDescriptor desc = new HTableDescriptor(tn);
		// configure to set KEEP_DELETED_CELLS => 'true'
		HColumnDescriptor descColFamily = new HColumnDescriptor(columnFamily);
		desc.addFamily(descColFamily);
		admin.createTable(desc);
	}

	public static void deleteTable(Configuration conf, byte[] tableName)
			throws IOException {
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(tableName);
		if (admin.isTableDisabled(tn)) {
			admin.deleteTable(tn);
		} else {
			admin.disableTable(tn);
			admin.deleteTable(tn);
		}

	}
}
