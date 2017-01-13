package ict.ictbase.test.local;

import ict.ictbase.commons.local.LocalHTableGetByIndex;
import ict.ictbase.util.HIndexConstantsAndUtils;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalIssueCompaction {
	public static final Log LOG = LogFactory
			.getLog(HIndexConstantsAndUtils.class);

	private static String testTableName = "testtable";
	private static String columnFamily = "cf";
	private static String indexedColumnName = "field0";
	private static String INDEXTABLE_COLUMNFAMILY = "INDEX_CF";

	private static Configuration conf;
	private static String coprocessorJarLoc = "hdfs://data8:9000/jar/ICTBase-1-0.0.1-SNAPSHOT.jar";
	private static LocalHTableGetByIndex htable;

	private static String startKeyStr = "a";
	private static String endKeyStr = "z";
	private static int numberOfRegions = 10;

	public static void initTables(Configuration conf, String testTableName,
			String columnFamily, String indexedColumnName, String startKeyStr,
			String endKeyStr, int numberOfRegions) throws Exception {
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(testTableName);

		if (admin.isTableAvailable(tn)) {
			HIndexConstantsAndUtils.deleteTable(conf,
					Bytes.toBytes(testTableName));
		}
		
		HIndexConstantsAndUtils.createAndConfigAndSplitBaseTable(conf,
				Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily),
				Bytes.toBytes(INDEXTABLE_COLUMNFAMILY),
				new String[] { indexedColumnName }, startKeyStr, endKeyStr,
				numberOfRegions);
	}

	public static void loadData() throws IOException {
		char tmpChar = 97;
		String tmpStr = null;
		byte[] rowKey = null;
		for (int i = 0; i < 20; i++) {
			tmpStr = String.valueOf((char) (tmpChar + i)+"KEY"+i);
			rowKey = Bytes.toBytes(tmpStr);
			Put p = new Put(rowKey);
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName), Bytes.toBytes("va" + (i)));
			htable.put(p);
		}
	}

	public static void loadData2MutilRegion() throws IOException {
		String rowStr = "";
		byte[] rowKey;
		for (int i = 0; i < 100; i++) {
			rowKey = Bytes.toBytes(rowStr + i);
			Put p = new Put(rowKey);
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName), Bytes.toBytes("vb" + i));
			htable.put(p);
		}
	}

	public static void initCoProcessors(Configuration conf,
			String coprocessorJarLoc, LocalHTableGetByIndex htable)
			throws IOException {
		int coprocessorIndex = 1;
		HIndexConstantsAndUtils
				.updateCoprocessor(conf, htable.getTableName(),
						coprocessorIndex++, true, coprocessorJarLoc,
						"ict.ictbase.coprocessor.local.LocalPhysicalDeletionInCompaction");

		HIndexConstantsAndUtils.updateCoprocessor(conf, htable.getTableName(),
				coprocessorIndex++, true, coprocessorJarLoc,
				"ict.ictbase.coprocessor.local.LocalIndexJusuPutObserver");

		HIndexConstantsAndUtils.updateCoprocessor(conf, htable.getTableName(),
				coprocessorIndex++, true, coprocessorJarLoc,
				"ict.ictbase.coprocessor.local.LocalIndexScanObserver");
	}

	public static void issueMajorCompactionAsynchronously() {
		try {
			Connection con = ConnectionFactory.createConnection(conf);
			Admin admin = con.getAdmin();
			TableName tn = TableName.valueOf(testTableName);
			admin.flush(tn);
			admin.majorCompact(tn, Bytes.toBytes(columnFamily));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void scanTest() throws IOException {
		Scan scan = new Scan();
		scan.setMaxVersions(Integer.MAX_VALUE);
		String startValue = "va13";
		String family = "cf";
		String qualifier = "country";
		String regionStartKey = "m";
		String startRow = regionStartKey + "#" + family + "#" + qualifier + "#"
				+ startValue + "#";
		System.out.println("*************** startRow: " + startRow);
		scan.setStartRow(Bytes.toBytes(startRow));
		String stopValue = null;
		FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		if (stopValue == null) { // point query
			Filter prefixFilter = new PrefixFilter(Bytes.toBytes(startRow));
			scan.setFilter(prefixFilter);
			fl.addFilter(prefixFilter);
		} else {
			String stopRow = regionStartKey + "#" + family + "#" + qualifier
					+ "#" + stopValue + "#";
			scan.setStopRow(Bytes.toBytes(stopRow));
			System.out.println("*************** stopRow: " + stopRow);
			scan.setStopRow(Bytes.toBytes(stopValue));
		}
		scan.setFilter(fl);

		Connection con = ConnectionFactory.createConnection(conf);
		TableName tn = TableName.valueOf(testTableName);
		Table table = con.getTable(tn);
		ResultScanner rs = table.getScanner(scan);

		Result r = null;
		while ((r = rs.next()) != null) {
			for (Cell cell : r.rawCells()) {
				System.out
						.println(String
								.format("pre row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
										Bytes.toString(CellUtil.cloneRow(cell)),
										Bytes.toString(CellUtil
												.cloneFamily(cell)), Bytes
												.toString(CellUtil
														.cloneQualifier(cell)),
										Bytes.toString(CellUtil
												.cloneValue(cell)), cell
												.getTimestamp()));
			}
		}

	}

	public static void deleteTest() throws IOException {
		Connection con = ConnectionFactory.createConnection(conf);
		TableName tn = TableName.valueOf(testTableName);

		String deleteRowKey = "key_compact_3";
		Delete delete = new Delete(Bytes.toBytes(deleteRowKey));
		Table table = con.getTable(tn);
		table.delete(delete);
	}

	public static void main(String[] args) throws Exception {

		conf = HBaseConfiguration.create();
		if (args.length == 3) {
			testTableName = args[0];
			columnFamily = args[1];
			indexedColumnName = args[2];

		}

		initTables(conf, testTableName, columnFamily,indexedColumnName,startKeyStr,endKeyStr,numberOfRegions);
		htable = new LocalHTableGetByIndex(conf, Bytes.toBytes(testTableName));
		initCoProcessors(conf, coprocessorJarLoc, htable);
//		loadData();

//		 issueMajorCompactionAsynchronously();
		
		
		
//		 List<String> res = htable.getByIndex(Bytes.toBytes(columnFamily),
//		 Bytes.toBytes(indexedColumnName), Bytes.toBytes("vb3"));

//		scanTest();
	}
}
