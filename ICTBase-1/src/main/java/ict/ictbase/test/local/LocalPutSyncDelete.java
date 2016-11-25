package ict.ictbase.test.local;

import ict.ictbase.commons.local.LocalHTableGetByIndex;
import ict.ictbase.util.HIndexConstantsAndUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalPutSyncDelete {
	private static String testTableName = "test_local_sync_delete";
	private static String columnFamily = "cf";
	private static String indexedColumnName = "country";
	private static String INDEXTABLE_COLUMNFAMILY = "INDEX_CF";
	
	private static Configuration conf;
	private static String coprocessorJarLoc = "hdfs://data8:9000/jar/ICTBase-1-0.0.1-SNAPSHOT.jar";
	private static LocalHTableGetByIndex htable;
	
	private static String startKeyStr = "a";
	private static String endKeyStr = "z";
	private static int numberOfRegions = 10;

	public static void initTables(Configuration conf, String testTableName,
			String columnFamily, String indexedColumnName,String startKeyStr,String endKeyStr,int numberOfRegions) throws Exception {
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(testTableName);

		if (admin.isTableAvailable(tn)) {
			HIndexConstantsAndUtils.deleteTable(conf,
					Bytes.toBytes(testTableName));
		}

		HIndexConstantsAndUtils.createAndConfigAndSplitBaseTable(conf,
				Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily),Bytes.toBytes(INDEXTABLE_COLUMNFAMILY),
				new String[] { indexedColumnName },startKeyStr,endKeyStr,numberOfRegions);
	}

	public static void initCoProcessors(Configuration conf,
			String coprocessorJarLoc, LocalHTableGetByIndex htable) throws Exception {
		int coprocessorIndex = 1;
		HIndexConstantsAndUtils.updateCoprocessor(conf, htable.getTableName(),
				coprocessorIndex++, true, coprocessorJarLoc,
				"ict.ictbase.coprocessor.local.LocalIndexBaselineObserver");
		
		HIndexConstantsAndUtils.updateCoprocessor(conf, htable.getTableName(),
				coprocessorIndex++, true, coprocessorJarLoc,
				"ict.ictbase.coprocessor.local.LocalIndexScanObserver");
	}

	public static void loadData() throws IOException {
		char tmpChar = 97;
		String tmpStr=null;
		byte[] rowKey = null;
		for (int i = 0; i < 10; i++) {
			tmpStr = String.valueOf((char)(tmpChar+i));
			rowKey = Bytes.toBytes(tmpStr);
			Put p = new Put(rowKey);
			long ts = 100+i;
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName), ts,
					Bytes.toBytes("100"));
			p.setAttribute("put_time_version", Bytes.toBytes(ts));
			htable.put(p);
		}
		Put p = new Put(Bytes.toBytes("n"));
		long ts = 120;
		p.addColumn(Bytes.toBytes(columnFamily),
				Bytes.toBytes(indexedColumnName), ts,
				Bytes.toBytes("111"));
		p.setAttribute("put_time_version", Bytes.toBytes(ts));
		htable.put(p);
	}
	
	public static void loadAndDeleteData() throws IOException {
		String tmpStr="e";
		byte[] rowKey = null;
		for (int i = 10; i < 20; i++) {
			rowKey = Bytes.toBytes(tmpStr);
			Put p = new Put(rowKey);
			long ts = 100+i;
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName), ts,
					Bytes.toBytes("v" + i));
			p.setAttribute("put_time_version", Bytes.toBytes(ts));
			htable.put(p);
		}
	}
	
	
	
	
	public static void getResultFromScan(LocalHTableGetByIndex htable){
		Scan scan  = new Scan();
		FilterList filterList  = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
				Bytes.toBytes(indexedColumnName),CompareOp.EQUAL,Bytes.toBytes("100"));
//		filter.setFilterIfMissing(true);
		filterList.addFilter(filter);
		scan.setFilter(filterList);
		scan.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(indexedColumnName));
	
//		/**************************************************/
//		 String startRow = Bytes.toString(scan.getStartRow());	
//		  String stopRow = Bytes.toString(scan.getStopRow());	
//		  System.out.println("********** startRow: "+startRow+",    stopRow: "+stopRow);
//		  byte[][] tmpBytes = scan.getFamilies();
//		  
//		  for(byte [] tb: tmpBytes){
//			  System.out.println("********** family:"+Bytes.toString(tb));
//		  }
//		  System.out.println("********** filter:"+ scan.getFilter().toString());
//		  Map<byte[], NavigableSet<byte[]>>  tmpMap = scan.getFamilyMap();
//		  
//		  for(Entry<byte[], NavigableSet<byte[]>> entry: tmpMap.entrySet()){
//			  System.out.println("********** entry.getKey:"+Bytes.toString(entry.getKey()));
//			  NavigableSet<byte[]> tmpV = entry.getValue();
//			  while(!tmpV.isEmpty()){
//				  System.out.println("********* map value "+Bytes.toString(tmpV.pollFirst()));
//			  }
//			  
//		  }
//		  /**************************************************/
		
		try {
			ResultScanner rs = htable.getScanner(scan);
			Result r;
			while((r=rs.next())!=null){
				for(Cell cell : r.listCells()){
					System.out.println("^^^^^^^^^^^^^^^^^^ getResultFromScan");
					System.out.println(String.format("row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
							Bytes.toString(CellUtil.cloneRow(cell)),
							Bytes.toString(CellUtil.cloneFamily(cell)),
							Bytes.toString(CellUtil.cloneQualifier(cell)),
							Bytes.toString(CellUtil.cloneValue(cell)),
							cell.getTimestamp()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	
	}

	public static void main(String[] args) throws Exception {
		conf = HBaseConfiguration.create();
		if (args.length == 3) {
			testTableName = args[0];
			columnFamily = args[1];
			indexedColumnName = args[2];

		}
		
		initTables(conf, testTableName, columnFamily, indexedColumnName,startKeyStr,endKeyStr,numberOfRegions);
		htable = new LocalHTableGetByIndex(conf, Bytes.toBytes(testTableName));
		initCoProcessors(conf, coprocessorJarLoc, htable);
		loadData();
		
//		loadAndDeleteData();
		
//		getResultFromScan(htable);
		
		List<String> res = htable.getByIndex(Bytes.toBytes(columnFamily),
				Bytes.toBytes(indexedColumnName), Bytes.toBytes("100"));
//		assert (res != null && res.size() != 0);
//		for(int i =0;i<res.size();i++){
//			System.out.println("Result is " + res.get(i));
//		}
		

	}
}
