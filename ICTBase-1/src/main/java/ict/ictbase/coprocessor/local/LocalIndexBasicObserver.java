package ict.ictbase.coprocessor.local;

import ict.ictbase.commons.local.LocalHTableUpdateIndexByPut;
import ict.ictbase.coprocessor.LoggedObserver;
import ict.ictbase.util.local.LocalQueueUtil;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalIndexBasicObserver extends LoggedObserver {
	private boolean initialized;
	private boolean initializedForPut;
	protected LocalHTableUpdateIndexByPut dataTableWithLocalIndexes = null;
	protected LocalQueueUtil localQueueUtil = null;

	private void tryInitialize(HTableDescriptor desc) throws IOException {
		if (initialized == false) {
			synchronized (this) {
				if (initialized == false) {
					Configuration conf = HBaseConfiguration.create();
					dataTableWithLocalIndexes = new LocalHTableUpdateIndexByPut(
							conf, desc.getTableName().getName());
					initialized = true;
				}
			}
		}
	}

	private void tryInitializeForPut(HTableDescriptor desc, String startKey,
			Region region) throws IOException {
		if (initializedForPut == false) {
			synchronized (this) {
				if (initializedForPut == false) {
					Configuration conf = HBaseConfiguration.create();
					dataTableWithLocalIndexes = new LocalHTableUpdateIndexByPut(
							conf, desc.getTableName().getName());
					localQueueUtil = new LocalQueueUtil(
							dataTableWithLocalIndexes, startKey, region);
					initializedForPut = true;
				}
			}
		}
	}

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		setFunctionLevelLogging(false);
		initialized = false;
		initializedForPut = false;
		super.start(e);
	}

	@Override
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Put put, final WALEdit edit, final Durability durability)
			throws IOException {
		super.prePut(e, put, edit, durability);
		Region region = e.getEnvironment().getRegion();
		HRegionInfo regionInfo = e.getEnvironment().getRegionInfo();
		String startKey = Bytes.toString(regionInfo.getStartKey());
		tryInitializeForPut(e.getEnvironment().getRegion().getTableDesc(),
				startKey, region);
	}

	@Override
	public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Put put, final WALEdit edit, final Durability durability)
			throws IOException {
		super.postPut(e, put, edit, durability);
	}

	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
			Delete delete, WALEdit edit, final Durability durability)
			throws IOException {
		super.preDelete(e, delete, edit, durability);
		tryInitialize(e.getEnvironment().getRegion().getTableDesc());
	}

	@Override
	public InternalScanner preCompact(
			ObserverContext<RegionCoprocessorEnvironment> e, final Store store,
			final InternalScanner scanner, final ScanType scanType)
			throws IOException {
		InternalScanner toRet = super.preCompact(e, store, scanner, scanType);
		tryInitialize(e.getEnvironment().getRegion().getTableDesc());
		return toRet;
	}

	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e,
			Get get, List<Cell> result) throws IOException {
		super.preGetOp(e, get, result);
		tryInitialize(e.getEnvironment().getRegion().getTableDesc());
	}

	@Override
	public boolean preScannerNext(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final InternalScanner s, final List<Result> results,
			final int limit, final boolean hasMore) throws IOException {
		super.preScannerNext(e, s, results, limit, hasMore);
		tryInitialize(e.getEnvironment().getRegion().getTableDesc());
		return hasMore;
	}

	@Override
	public boolean postScannerNext(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final InternalScanner s, final List<Result> results,
			final int limit, final boolean hasMore) throws IOException {
		super.postScannerNext(e, s, results, limit, hasMore);
		tryInitialize(e.getEnvironment().getRegion().getTableDesc());
		return hasMore;
	}

	@Override
	public RegionScanner preScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Scan scan, final RegionScanner s) throws IOException {
		super.preScannerOpen(e, scan, s);
		return s;
	}

	@Override
	public InternalScanner preFlush(
			ObserverContext<RegionCoprocessorEnvironment> e, Store store,
			InternalScanner scanner) throws IOException {
		tryInitialize(e.getEnvironment().getRegion().getTableDesc());
		super.preFlush(e, store, scanner);
		return scanner;
	}

	@Override
	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e,
			Store store, StoreFile resultFile) throws IOException {
		tryInitialize(e.getEnvironment().getRegion().getTableDesc());
		super.postFlush(e, store, resultFile);
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		super.stop(e);
		if (dataTableWithLocalIndexes != null) {
			dataTableWithLocalIndexes.close();
		}
	}
}
