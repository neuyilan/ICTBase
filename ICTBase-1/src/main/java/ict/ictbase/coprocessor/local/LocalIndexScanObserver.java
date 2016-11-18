package ict.ictbase.coprocessor.local;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalIndexScanObserver extends LocalIndexBasicObserver {

	@Override
	public boolean preScannerNext(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final InternalScanner s, final List<Result> results,
			final int limit, final boolean hasMore) throws IOException {
		
		
		for(Result r: results){
			for(Cell cell : r.rawCells()){
				System.out.println("^^^^^^^^^^^^^^^^^^ preScannerNext");
				System.out.println(String.format("row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
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
		for(Result r: results){
			for(Cell cell : r.rawCells()){
				System.out.println("^^^^^^^^^^^^^^^^^^ postScannerNext");
				System.out.println(String.format("row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
						Bytes.toString(CellUtil.cloneRow(cell)),
						Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)),
						Bytes.toString(CellUtil.cloneValue(cell)),
						cell.getTimestamp()));
			}
		}
		return hasMore;

	}

}
