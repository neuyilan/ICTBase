package ict.ictbase.coprocessor.global;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class IndexObserverAsyncMaintain extends BasicIndexObserver {
	@Override
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Put put, final WALEdit edit, final Durability durability)
			throws IOException {
		super.prePut(e, put, edit, durability);
//		long now = EnvironmentEdgeManager.currentTime();
		long now = queueUtil.getNowTime();
		
		byte[] byteNow = Bytes.toBytes(now);
		Map<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
		for (Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
//			System.out.println(Bytes.toString(entry.getKey()));

			List<Cell> cells = entry.getValue();
			for (Cell cell : cells) {
				CellUtil.updateLatestStamp(cell, byteNow, 0);
				System.out.println("*******"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ","
						+ Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
						+ Bytes.toString(CellUtil.cloneValue(cell)) + ","
						+ Bytes.toString(CellUtil.cloneRow(cell)));

			}

		}
		put.setAttribute("put_time_version", Bytes.toBytes(now));
		
		System.out.println("*********put.toJSON(): "+put.toJSON());
		queueUtil.addTablePutQueueMap(put);
	}
	
//	@Override
//	public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e,
//			final Put put, final WALEdit edit, final Durability durability)
//			throws IOException {
//		super.postPut(e, put, edit, durability);
//		queueUtil.addTablePutQueueMap(put);
//	}

}
