package ict.ictbase.timebreakdown;
import ict.ictbase.commons.global.GlobalHTableGetByIndex;
import ict.ictbase.util.HIndexConstantsAndUtils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class GlobalTimeBreakDownTest{
	private static String testTableName = "timetesttable";
	private static String columnFamily = "cf";
	private static String indexedColumnName = "field0";
	private static Configuration conf;
	private static String coprocessorJarLoc = "hdfs://data8:9000/jar/ICTBase-1-0.0.1-SNAPSHOT.jar";
	private static GlobalHTableGetByIndex htable;

	public static void initTables(Configuration conf, String testTableName,
			String columnFamily, String indexedColumnName) throws Exception {
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(testTableName);

		if (admin.isTableAvailable(tn)) {
			System.out.println("table exist,delete it first");
			HIndexConstantsAndUtils.deleteTable(conf,
					Bytes.toBytes(testTableName));
		}

		HIndexConstantsAndUtils.createAndConfigBaseTable(conf,
				Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily),
				new String[] { indexedColumnName });

		byte[] indexTableName = HIndexConstantsAndUtils.generateIndexTableName(
				Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily),
				Bytes.toBytes(indexedColumnName));

		TableName indexTN = TableName.valueOf(indexTableName);

		if (admin.isTableAvailable(indexTN)) {
			System.out.println("index table exist,delete it first");
			HIndexConstantsAndUtils.deleteTable(conf, indexTableName);
		}

		HIndexConstantsAndUtils.createAndConfigIndexTable(conf, indexTableName,
				Bytes.toBytes(columnFamily));
		
//		initIndexTableCoProcessors(conf,coprocessorJarLoc,indexTableName);
	}

	public static void initCoProcessors(Configuration conf,
			String coprocessorJarLoc, GlobalHTableGetByIndex htable) throws Exception {
		int coprocessorIndex = 1;
		HIndexConstantsAndUtils.updateCoprocessor(conf, htable.getTableName(),
				coprocessorIndex++, true, coprocessorJarLoc,
				"ict.ictbase.coprocessor.global.GlobalIndexObserverBaseline");
	}

	
//	public static void initIndexTableCoProcessors(Configuration conf,
//			String coprocessorJarLoc, byte[] htable) throws Exception {
//		int coprocessorIndex = 1;
//		HIndexConstantsAndUtils.updateCoprocessor(conf, htable,
//				coprocessorIndex++, true, coprocessorJarLoc,
//				"ict.ictbase.coprocessor.global.GlobalIndexScanObserver");
//	}
	
	
	public static void loadData() throws IOException {
		String rowkeyStr = "key_time";
		byte[] rowKey = Bytes.toBytes(rowkeyStr);
		for (int i = 0; i < 10000; i++) {
			Put p = new Put(rowKey);
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName),
					Bytes.toBytes("value"));
			htable.put(p);
		}

	}

	

	public static void loadData2DiffKey() throws IOException {
		String tmpStr="sync_diff";
		byte[] rowKey = null;
		for (int i = 0; i < 10000; i++) {
			rowKey = Bytes.toBytes(tmpStr+i);
			System.out.println("start rpc: "+System.nanoTime());
			Put p = new Put(rowKey);
			long value = 100+i;
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName),
					Bytes.toBytes("va"+value));
			htable.put(p);
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
		htable = new GlobalHTableGetByIndex(conf, Bytes.toBytes(testTableName));
		initCoProcessors(conf, coprocessorJarLoc, htable);

		htable = new GlobalHTableGetByIndex(conf, Bytes.toBytes(testTableName));
		loadData();
		
	}
}
