package ict.ictbase.commons.local;

import ict.ictbase.util.HIndexConstantsAndUtils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HTableUpdateLocalIndexByPut extends HTableWithLocalIndexesDriver {
	public HTableUpdateLocalIndexByPut(Configuration conf, byte[] tableName)
			throws IOException {
		super(conf, tableName);
	}

	final static private int INSERT_INDEX = 0;
	final static private int READ_BASE = 1;
	final static private int DELETE_INDEX = 2;

	public void insertNewToIndexes(Put put, String regionStartKey)
			throws IOException {
		internalPrimitivePerPut(put, INSERT_INDEX, null, regionStartKey);
		// String indexTable = Bytes.toString(this.getTableName());
		// HRegionInfo regionInfo = e.getEnvironment().getRegionInfo();
		// String regionSatrtKey = Bytes.toString(regionInfo.getStartKey());
		// String index = put.get
		// Put put = new Put();
		// policyToMaterializeIndex.putToIndex(indexTable, dataValue, dataKey);
		//
	}

	private Result internalPrimitivePerPut(Put put, int mode,
			Result readResult4Delete, String regionStartKey) throws IOException {
		HTableDescriptor dataTableDesc = null;
		try {
			dataTableDesc = getTableDescriptor();
		} catch (IOException e1) {
			throw new RuntimeException("TTERROR" + (errorIndex++) + "_DETAIL: "
					+ e1.getMessage());
		}
		byte[] dataKey = put.getRow();
		Get get = null;
		if (mode == READ_BASE) {
			get = new Get(dataKey);
		}
		for (int index = 1;; index++) {
			String fullpathOfIndexedcolumnInDatatable = dataTableDesc
					.getValue(HIndexConstantsAndUtils.INDEX_INDICATOR + index);
			if (fullpathOfIndexedcolumnInDatatable == null) {
				// no (further) index column, stop at current index
				break;
			} else {
				String[] datatableColumnPath = fullpathOfIndexedcolumnInDatatable
						.split("\\|");
				byte[] indexedColumnFamily = Bytes
						.toBytes(datatableColumnPath[0]);
				byte[] indexedColumnName = Bytes
						.toBytes(datatableColumnPath[1]);
				byte[] dataValuePerColumn = getColumnValue(put,
						indexedColumnFamily, indexedColumnName);
				if (dataValuePerColumn != null) {
					if (mode == INSERT_INDEX) {
						// put new to index
						putToIndex(regionStartKey,indexedColumnFamily, indexedColumnName,
								dataValuePerColumn, dataKey);
					} else if (mode == READ_BASE) {
						// read base
						// TOREMOVE need specify timestamp to guarantee get old
						// values.
						long maxTs = Bytes.toLong(put
								.getAttribute("put_time_version"));
						get.setTimeRange(0, maxTs);
						get.setMaxVersions();
						get.addColumn(indexedColumnFamily, indexedColumnName);
					} else {
					}
				} else {
					// the indexed column (family) is not associated with the
					// put, to continue.
					continue;
				}
			}
		}
		if (mode == READ_BASE) {
			Result readResultOld = this.get(get);
			return readResultOld;
		} else {
			return null;
		}
	}

	protected byte[] getColumnValue(final Put put, byte[] columnFamily,
			byte[] columnName) {
		if (!put.has(columnFamily, columnName)) {
			return null;
		}

		List<Cell> values = put.get(columnFamily, columnName);
		if (values == null || values.isEmpty()) {
			throw new RuntimeException("TTERROR_" + (errorIndex++) + ": "
					+ "empty value lists while put.has() returns true!");
		}

		// should be one element in values, since column qualifier is an exact
		// name, matching one column; also one version of value is expected.
		if (values.size() != 1) {
			throw new RuntimeException(
					"TTERROR_"
							+ (errorIndex++)
							+ ": "
							+ "multiple versions of values or multiple columns by qualier in put()!");
		}

		// TOREMOVE to get timestamp, refer to old project code.
		Cell cur = values.get(0);
		byte[] value = CellUtil.cloneValue(cur);
		return value;
	}
}
