package ict.ictbase.coprocessor.local;

import java.io.IOException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalIndexBaselineObserver extends LocalIndexBasicObserver{
	
	@Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put, final WALEdit edit, final Durability durability) throws IOException {
		super.prePut(e, put, edit, durability);
		HRegionInfo regionInfo =  e.getEnvironment().getRegionInfo();
		String regionStartKey = Bytes.toString(regionInfo.getStartKey());
		dataTableWithLocalIndexes.insertNewToIndexes(put,regionStartKey);
		dataTableWithLocalIndexes.readBaseAndDeleteOld(put,regionStartKey);
    }
}
