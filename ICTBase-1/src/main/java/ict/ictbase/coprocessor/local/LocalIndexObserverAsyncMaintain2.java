package ict.ictbase.coprocessor.local;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class LocalIndexObserverAsyncMaintain2 extends LocalIndexBasicObserver {
	Region region ;//= e.getEnvironment().getRegion();
	HRegionInfo regionInfo ;//=  e.getEnvironment().getRegionInfo();
	String regionStartKey ;//= Bytes.toString(regionInfo.getStartKey());
	
	@Override
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Put put, final WALEdit edit, final Durability durability)
			throws IOException {
		super.prePut(e, put, edit, durability);
//		long now = localQueueUtil.getNowTime();
		long now = EnvironmentEdgeManager.currentTime();
		byte[] byteNow = Bytes.toBytes(now);
		Map<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
		for (Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
			List<Cell> cells = entry.getValue();
			for (Cell cell : cells) {
				CellUtil.updateLatestStamp(cell, byteNow, 0);
			}
		}
		put.setAttribute("put_time_version", Bytes.toBytes(now));
		/************************************************************/
		if(put.getAttribute("index_put")==null){
			System.out.println("a put: "
					+ Bytes.toLong(put.getAttribute("put_time_version")));
		}
		/************************************************************/
		
	}
	
	public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Put put, final WALEdit edit, final Durability durability)
			throws IOException {
		region = e.getEnvironment().getRegion();
		regionInfo =  e.getEnvironment().getRegionInfo();
		regionStartKey = Bytes.toString(regionInfo.getStartKey());
		
		dataTableWithLocalIndexes.insertNewToIndexes(put,regionStartKey,region);
		
		localQueueUtil.addTablePutQueueMap(put);
	}
}
