package ict.ictbase.test.local;

import ict.ictbase.commons.local.LocalHTableGetByIndex;
import ict.ictbase.util.HIndexConstantsAndUtils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

// local async_simple
public class LocalPutAsyncDelete {
	private static String testTableName = "testtable";
	private static String columnFamily = "cf";
	private static String indexedColumnName = "field0";
	private static Configuration conf;
	private static String coprocessorJarLoc = "hdfs://data8:9000/jar/ICTBase-1-0.0.1-SNAPSHOT.jar";
	private static LocalHTableGetByIndex htable;

	
	private static String startKeyStr = "a";
	private static String endKeyStr = "z";
	private static int numberOfRegions = 10;
	private static String INDEXTABLE_COLUMNFAMILY = "INDEX_CF";
	
	public static void initTables(Configuration conf, String testTableName,
			String columnFamily, String indexedColumnName) throws Exception {
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
				"ict.ictbase.coprocessor.local.LocalIndexObserverAsyncMaintain");
		
//		HIndexConstantsAndUtils.updateCoprocessor(conf, htable.getTableName(),
//				coprocessorIndex++, true, coprocessorJarLoc,
//				"ict.ictbase.coprocessor.local.LocalIndexScanObserver");
	}

	public static void loadData() throws IOException {
		// load data
		String rowkeyStr = "global_key";
		byte[] rowKey = Bytes.toBytes(rowkeyStr);
		for (int i = 0; i < 100; i++) {
			Put p = new Put(rowKey);
			long value = i;
			
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName),
					Bytes.toBytes("va" + value));
			
			htable.put(p);
		}
	}
	
	
	public static void loadData2MutilRegion() throws IOException {
		char tmpChar = 97;
		String tmpStr=null;
		byte[] rowKey = null;
		for (int i = 0; i < 20; i++) {
			tmpStr = String.valueOf((char)(tmpChar+i));
			rowKey = Bytes.toBytes(tmpStr);
			Put p = new Put(rowKey);
			long ts = 100+i;
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName), ts,
					Bytes.toBytes("value"+ts));
			p.setAttribute("put_time_version", Bytes.toBytes(ts));
			htable.put(p);
		}
	}
	
	public static void getTest() throws IOException{
		String getRow = "aaa";
		Get get = new Get(Bytes.toBytes(getRow));
		Result  r = htable.get(get);
		System.out.println(r.isEmpty());
		if(r!=null){
			System.out.println(r.toString());
		}else{
			System.out.println("r is null");
		}
	}
	

	public static void main(String[] args) throws Exception {
		conf = HBaseConfiguration.create();
		if (args.length == 3) {
			testTableName = args[0];
			columnFamily = args[1];
			indexedColumnName = args[2];

		}
		initTables(conf, testTableName, columnFamily, indexedColumnName);
		htable = new LocalHTableGetByIndex(conf, Bytes.toBytes(testTableName));
		initCoProcessors(conf, coprocessorJarLoc, htable);
//		loadData();

		
//		loadData2MutilRegion();
		
		// getByIndex
//		List<String> res = htable.getByIndex(Bytes.toBytes(columnFamily),
//				Bytes.toBytes(indexedColumnName), Bytes.toBytes("v10"));
//		assert (res != null && res.size() != 0);
		
	}
}
