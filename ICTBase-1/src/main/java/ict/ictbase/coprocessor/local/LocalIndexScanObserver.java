package ict.ictbase.coprocessor.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalIndexScanObserver extends LocalIndexBasicObserver {
	
    static final public String  SCAN_INDEX_FAMILIY="scan_index_family";
    static final public String  SCAN_INDEX_QUALIFIER="scan_index_qualifier";
    static final public String  SCAN_START_VALUE="scan_start_value";
    static final public String  SCAN_STOP_VALUE="scan_stop_value";
    
	@Override
	public boolean preScannerNext(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final InternalScanner s, final List<Result> results,
			final int limit, final boolean hasMore) throws IOException {
		super.preScannerNext(e, s, results, limit, hasMore);
		HRegionInfo hreginInfo = e.getEnvironment().getRegionInfo();
		System.out.println("********** hreginInfo.getStartKey: "+Bytes.toString(hreginInfo.getStartKey()));
		for(Result r: results){
			for(Cell cell : r.rawCells()){
				System.out.println(String.format("pre row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
						Bytes.toString(CellUtil.cloneRow(cell)),
						Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)),
						Bytes.toString(CellUtil.cloneValue(cell)),
						cell.getTimestamp()));
			}
		}
		
		return hasMore;

	}
	
	@Override
	public boolean postScannerNext(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final InternalScanner s, final List<Result> results,
			final int limit, final boolean hasMore) throws IOException {
		super.postScannerNext(e, s, results, limit, hasMore);
		
		TableName tableName = e.getEnvironment().getRegionInfo().getTable();
		Configuration conf = e.getEnvironment().getConfiguration();
		List<Result> retResultList = new ArrayList<Result>();	
		Result tmpResult = null;
		for(Result r: results){
			for(Cell cell : r.rawCells()){
				System.out.println(String.format("post row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
						Bytes.toString(CellUtil.cloneRow(cell)),
						Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)),
						Bytes.toString(CellUtil.cloneValue(cell)),
						cell.getTimestamp()));
				
				String tmpRowKey = Bytes.toString(CellUtil.cloneRow(cell));
				String arr[] = tmpRowKey.split("#");
				String rowKey  = arr[arr.length-1];
				tmpResult = this.getResultFromDataTable(conf, tableName, rowKey);
				if(tmpResult!=null){
					retResultList.add(tmpResult);
				}
			}
		}
		results.clear();
		results.addAll(retResultList);
		return hasMore;

	}
	
	public Result getResultFromDataTable(Configuration conf,TableName tableName,String rowKey){
		Connection con;
		Table dataTable;
		Result result = null;
		try {
			con = ConnectionFactory.createConnection(conf);
			dataTable = con.getTable(tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			result = dataTable.get(get);
			
			for(Cell cell : result.rawCells()){
				System.out.println(String.format("getResultFromDataTable row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
						Bytes.toString(CellUtil.cloneRow(cell)),
						Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)),
						Bytes.toString(CellUtil.cloneValue(cell)),
						cell.getTimestamp()));
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	  @Override
	  public RegionScanner preScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e,
	      final Scan scan, final RegionScanner s) throws IOException {
		  
		  HRegionInfo hregionInfo =  e.getEnvironment().getRegionInfo();
		  String regionStartKey = Bytes.toString(hregionInfo.getStartKey());
		  
		  String startValue = Bytes.toString(scan.getAttribute(SCAN_START_VALUE));
		  String stopValue = Bytes.toString(scan.getAttribute(SCAN_STOP_VALUE));
		  
		  String family = Bytes.toString(scan.getAttribute(SCAN_INDEX_FAMILIY));
		  String qualifier = Bytes.toString(scan.getAttribute(SCAN_INDEX_QUALIFIER));
		  
		  String startRow = regionStartKey+"#"+family+"#"+qualifier+"#"+startValue+"#";
		  System.out.println("*************** startRow: "+startRow);
		  
		  FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		  
		 
		  scan.setStartRow(Bytes.toBytes(startRow));
		  
		  if (stopValue == null){ //point query
	            Filter prefixFilter  = new PrefixFilter(Bytes.toBytes(startRow));	
	  		  	scan.setFilter(prefixFilter);
	            fl.addFilter(prefixFilter);
	        } else {
	        	String stopRow  = regionStartKey+"#"+family+"#"+qualifier+"#"+stopValue+"#";
				scan.setStopRow(Bytes.toBytes(stopRow));
				System.out.println("*************** stopRow: "+stopRow);
		        scan.setStopRow(Bytes.toBytes(stopValue));
	        }
		  super.preScannerOpen(e, scan, s);
		  return s;
	  }

}
