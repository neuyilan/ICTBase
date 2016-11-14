package ict.ictbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

public class IndexObserverAsyncMaintain extends BasicIndexObserver {

	@Override
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Put put, final WALEdit edit, final Durability durability)
			throws IOException {
		super.prePut(e, put, edit, durability);
//		dataTableWithIndexes.insertNewToIndexes(put);
		queueUtil.addTablePutQueueMap(dataTableWithIndexes, put);
	}
}
