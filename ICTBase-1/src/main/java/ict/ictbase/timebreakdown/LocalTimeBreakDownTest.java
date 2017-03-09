package ict.ictbase.timebreakdown;

import ict.ictbase.commons.local.LocalHTableGetByIndex;
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

public class LocalTimeBreakDownTest {
	private static String testTableName = "local_time_table";
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
		
//		HIndexConstantsAndUtils.updateCoprocessor(conf, htable.getTableName(),
//				coprocessorIndex++, true, coprocessorJarLoc,
//				"ict.ictbase.coprocessor.local.LocalIndexScanObserver");
	}

	public static void loadData() throws IOException {
		String rowStr = "key_time";
		byte[] rowKey = Bytes.toBytes(rowStr);
		for (int i = 0; i < 1; i++) {
			Put p = new Put(rowKey);
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName), Bytes.toBytes("value"));
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
		
//		initTables(conf, testTableName, columnFamily, indexedColumnName,startKeyStr,endKeyStr,numberOfRegions);
//		htable = new LocalHTableGetByIndex(conf, Bytes.toBytes(testTableName));
//		initCoProcessors(conf, coprocessorJarLoc, htable);
		
		htable = new LocalHTableGetByIndex(conf, Bytes.toBytes(testTableName));
		loadData();
		
		

	}
}
