package ict.ictbase.coprocessor.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;

public class LocalIndexScanObserver extends LocalIndexBasicObserver {

	static final public String SCAN_INDEX_FAMILIY = "scan_index_family";
	static final public String SCAN_INDEX_QUALIFIER = "scan_index_qualifier";
	static final public String SCAN_START_VALUE = "scan_start_value";
	static final public String SCAN_STOP_VALUE = "scan_stop_value";

	
	TableName tableName = null;//e.getEnvironment().getRegionInfo().getTable();
	Configuration conf = null;//e.getEnvironment().getConfiguration();
	Region region = null;//e.getEnvironment().getRegion();
	HRegionInfo hregionInfo = null;// = e.getEnvironment().getRegionInfo();
	
	
//	@Override
//	public boolean preScannerNext(
//			final ObserverContext<RegionCoprocessorEnvironment> e,
//			final InternalScanner s, final List<Result> results,
//			final int limit, final boolean hasMore) throws IOException {
//		super.preScannerNext(e, s, results, limit, hasMore);
//		HRegionInfo hreginInfo = e.getEnvironment().getRegionInfo();
//		System.out.println("********** hreginInfo.getStartKey: "
//				+ Bytes.toString(hreginInfo.getStartKey()));
//		for (Result r : results) {
//			for (Cell cell : r.rawCells()) {
//				System.out
//						.println(String
//								.format("pre row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
//										Bytes.toString(CellUtil.cloneRow(cell)),
//										Bytes.toString(CellUtil
//												.cloneFamily(cell)), Bytes
//												.toString(CellUtil
//														.cloneQualifier(cell)),
//										Bytes.toString(CellUtil
//												.cloneValue(cell)), cell
//												.getTimestamp()));
//			}
//		}
//
//		return hasMore;
//
//	}

	@Override
	public boolean postScannerNext(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final InternalScanner s, final List<Result> results,
			final int limit, final boolean hasMore) throws IOException {
		super.postScannerNext(e, s, results, limit, hasMore);

		tableName = e.getEnvironment().getRegionInfo().getTable();
		conf = e.getEnvironment().getConfiguration();
		region = e.getEnvironment().getRegion();

//		long attributeValue = Long.valueOf(e.getEnvironment().getConfiguration().get("scan_time_version"));
		
		long attributeValue = Bytes.toLong(s.getScan().getAttribute(
				"scan_time_version"));
		
		if(results==null){
			System.out.println("b scan: "+ attributeValue);
		}else if(results.isEmpty()){
			System.out.println("b scan: "+ attributeValue);
		} else{
			List<Result> retResultList = new ArrayList<Result>();
			Result tmpResult = null;
			for (Result r : results) {
				for (Cell cell : r.rawCells()) {
					String tmpRowKey = Bytes.toString(CellUtil.cloneRow(cell));
					String arr[] = tmpRowKey.split("#");
					String rowKey = arr[arr.length - 1];
					tmpResult = this.getResultFromDataTable(conf, tableName,
							rowKey, region,attributeValue);
					if (tmpResult != null) {
						retResultList.add(tmpResult);
					}
				}
			}
			results.clear();
			results.addAll(retResultList);
		}
		return hasMore;

	}

	public Result getResultFromDataTable(Configuration conf,
			TableName tableName, String rowKey, Region region,long attributeValue) {
		Result result = null;
		try {
			Get get = new Get(Bytes.toBytes(rowKey));
			get.setAttribute("get_time_version", Bytes.toBytes(attributeValue));
			result = region.get(get);
//			for (Cell cell : result.rawCells()) {
//				System.out
//				.println(String
//						.format("pre row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
//								Bytes.toString(CellUtil.cloneRow(cell)),
//								Bytes.toString(CellUtil
//										.cloneFamily(cell)), Bytes
//										.toString(CellUtil
//												.cloneQualifier(cell)),
//								Bytes.toString(CellUtil
//										.cloneValue(cell)), cell
//										.getTimestamp()));
//			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public RegionScanner preScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Scan scan, final RegionScanner s) throws IOException {

		hregionInfo = e.getEnvironment().getRegionInfo();
		String regionStartKey = Bytes.toString(hregionInfo.getStartKey());

		String startValue = Bytes.toString(scan.getAttribute(SCAN_START_VALUE));
		String stopValue = Bytes.toString(scan.getAttribute(SCAN_STOP_VALUE));

		String family = Bytes.toString(scan.getAttribute(SCAN_INDEX_FAMILIY));
		String qualifier = Bytes.toString(scan
				.getAttribute(SCAN_INDEX_QUALIFIER));

		String startRow = regionStartKey + "#" + family + "#" + qualifier + "#"
				+ startValue + "#";
//		System.out.println("*************** startRow: " + startRow);

		scan.setStartRow(Bytes.toBytes(startRow));

		String stopRow = null;
		if (stopValue == null) {
			stopRow = regionStartKey + "#" + family + "#" + qualifier + "#"
					+ startValue + "'" + "#";
		} else {
			stopRow = regionStartKey + "#" + family + "#" + qualifier + "#"
					+ stopValue + "#";
		}
		scan.setStopRow(Bytes.toBytes(stopRow));
//		System.out.println("*************** stopRow: " + stopRow);
		
		
		/***********************************************************/
//		IncrementingEnvironmentEdge IEE = new IncrementingEnvironmentEdge();
//		long now = IEE.currentTime();
		long now = EnvironmentEdgeManager.currentTime();
		byte[] byteNow = Bytes.toBytes(now);
		scan.setAttribute("scan_time_version", byteNow);
//		e.getEnvironment().getConfiguration().set("scan_time_version", String.valueOf(now));
		
		System.out.println("a scan: "+ Bytes.toLong(scan.getAttribute("scan_time_version")));
		/***********************************************************/
		super.preScannerOpen(e, scan, s);
		return s;
	}
	
	@Override
	public RegionScanner postScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Scan scan, final RegionScanner s) throws IOException {
		s.setScan(scan);
		return s;
	}
	
	@Override
	public void postGetOp(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Get get, final List<Cell> results) throws IOException {
		/************************************************************/
		if (get.getAttribute("get_time_version") == null) {
			// do nothing
		} else {
			System.out
					.println("b scan: "
							+ Bytes.toLong(get.getAttribute("get_time_version")));
		}

		/************************************************************/
	}
	
}
