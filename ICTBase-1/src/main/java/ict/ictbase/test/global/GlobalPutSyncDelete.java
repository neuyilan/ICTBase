package ict.ictbase.test.global;

import ict.ictbase.commons.global.GlobalHTableGetByIndex;
import ict.ictbase.util.HIndexConstantsAndUtils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class GlobalPutSyncDelete {
	private static String testTableName = "test_sync_delete";
	private static String columnFamily = "cf";
	private static String indexedColumnName = "country";
	private static Configuration conf;
	private static String coprocessorJarLoc = "hdfs://data8:9000/jar/ICTBase-1-0.0.1-SNAPSHOT.jar";
	private static GlobalHTableGetByIndex htable;

	public static void initTables(Configuration conf, String testTableName,
			String columnFamily, String indexedColumnName) throws Exception {
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(testTableName);

		if (admin.isTableAvailable(tn)) {
			HIndexConstantsAndUtils.deleteTable(conf,
					Bytes.toBytes(testTableName));
			System.out.println("*******************");
		}

		HIndexConstantsAndUtils.createAndConfigBaseTable(conf,
				Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily),
				new String[] { indexedColumnName });

		byte[] indexTableName = HIndexConstantsAndUtils.generateIndexTableName(
				Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily),
				Bytes.toBytes(indexedColumnName));

		TableName indexTN = TableName.valueOf(indexTableName);

		if (admin.isTableAvailable(indexTN)) {
			System.out.println("**********5555555*********");
			HIndexConstantsAndUtils.deleteTable(conf, indexTableName);
		}

		HIndexConstantsAndUtils.createAndConfigIndexTable(conf, indexTableName,
				Bytes.toBytes(columnFamily));
	}

	public static void initCoProcessors(Configuration conf,
			String coprocessorJarLoc, GlobalHTableGetByIndex htable) throws Exception {
		int coprocessorIndex = 1;
		HIndexConstantsAndUtils.updateCoprocessor(conf, htable.getTableName(),
				coprocessorIndex++, true, coprocessorJarLoc,
				"ict.ictbase.coprocessor.global.IndexObserverBaseline");

	}

	public static void loadData() throws IOException {
		// load data
		String rowkeyStr = "key_sync";
		byte[] rowKey = Bytes.toBytes(rowkeyStr);
		for (int i = 0; i < 10000; i++) {
			Put p = new Put(rowKey);
			long value = i;
//			long ts=System.currentTimeMillis();
//			p.addColumn(Bytes.toBytes(columnFamily),
//					Bytes.toBytes(indexedColumnName), ts,
//					Bytes.toBytes("va" + value));
//			p.setAttribute("put_time_version", Bytes.toBytes(ts));
			
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName),
					Bytes.toBytes("va" + value));
			htable.put(p);
		}

	}

	

	public static void loadData2DiffKey() throws IOException {
		String tmpStr="sync_diff";
		byte[] rowKey = null;
		for (int i = 0; i < 10000; i++) {
			rowKey = Bytes.toBytes(tmpStr+i);
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

//		loadData();
		loadData2DiffKey();

		// getByIndex
		htable.configPolicy(GlobalHTableGetByIndex.PLY_FASTREAD);
		List<byte[]> res = htable.getByIndex(Bytes.toBytes(columnFamily),
				Bytes.toBytes(indexedColumnName), Bytes.toBytes("v4"));
		assert (res != null && res.size() != 0);
		System.out.println("Result is " + Bytes.toString(res.get(0)));
	}
}
