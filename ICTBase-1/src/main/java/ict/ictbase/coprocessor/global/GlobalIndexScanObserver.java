package ict.ictbase.coprocessor.global;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class GlobalIndexScanObserver extends BaseRegionObserver {

	private boolean initialized;

	Configuration conf = null;
	Connection con = null;
	Table table = null;
	TableName tn;

	private void init() {
		if (initialized == false) {
			// System.out.println("******** come in the init method in GlobalIndexScanObserver");
			synchronized (this) {
				if (initialized == false) {
					conf = HBaseConfiguration.create();
					try {
						con = ConnectionFactory.createConnection(conf);
						tn = TableName.valueOf("testtable");
						table = con.getTable(tn);
					} catch (IOException e) {
						e.printStackTrace();
					}
					initialized = true;
				}
			}
		}
	}

	@Override
	public RegionScanner preScannerOpen(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final Scan scan, final RegionScanner s) throws IOException {
		// IncrementingEnvironmentEdge IEE = new IncrementingEnvironmentEdge();
		// long now = IEE.currentTime();
		long now = EnvironmentEdgeManager.currentTime();
		byte[] byteNow = Bytes.toBytes(now);
		scan.setAttribute("scan_time_version", byteNow);
		// e.getEnvironment().getConfiguration()
		// .set("scan_time_version", String.valueOf(now));


		System.out.println("a scan: "
				+ Bytes.toLong(scan.getAttribute("scan_time_version")));
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
	public boolean postScannerNext(
			final ObserverContext<RegionCoprocessorEnvironment> e,
			final InternalScanner s, final List<Result> results,
			final int limit, final boolean hasMore) throws IOException {
		// long attributeValue = Long.valueOf(e.getEnvironment()
		// .getConfiguration().get("scan_time_version"));

		long attributeValue = Bytes.toLong(s.getScan().getAttribute(
				"scan_time_version"));

		List<Result> retResultList = new ArrayList<Result>();
		Result tmpResult = null;
		if (results == null) {
			System.out.println("b scan: " + attributeValue);
		} else if (results.isEmpty()) {
			System.out.println("b scan: " + attributeValue);
		} else {
			for (Result r : results) {
				for (Cell cell : r.rawCells()) {
					String tmpRowKey = Bytes.toString(CellUtil.cloneRow(cell));
					String arr[] = tmpRowKey.split("/");
					String rowKey = arr[arr.length - 1];
					tmpResult = this.getResultFromDataTable(rowKey,
							attributeValue);
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

	public Result getResultFromDataTable(String rowKey, long attributeValue) {
		Result r = null;
		try {
			init();
			Get g = new Get(Bytes.toBytes(rowKey));
			g.setAttribute("get_time_version", Bytes.toBytes(attributeValue));
			r = table.get(g);

			// for (Cell cell : r.rawCells()) {
			// System.out
			// .println(String
			// .format(" row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
			// Bytes.toString(CellUtil.cloneRow(cell)),
			// Bytes.toString(CellUtil
			// .cloneFamily(cell)), Bytes
			// .toString(CellUtil
			// .cloneQualifier(cell)),
			// Bytes.toString(CellUtil
			// .cloneValue(cell)), cell
			// .getTimestamp()));
			// }

		} catch (IOException e) {
			e.printStackTrace();
		}
		return r;
	}

	public void close() {
		if (table != null) {
			try {
				table.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		if (con != null) {
			try {
				con.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}

}
