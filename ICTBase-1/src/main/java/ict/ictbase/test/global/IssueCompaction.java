package ict.ictbase.test.global;

/*
 disable 'testtable2'
 drop 'testtable2'
 create 'testtable2', {NAME=>'cf', KEEP_DELETED_CELLS=>true}
 disable 'testtable2_cf_country'
 drop 'testtable2_cf_country'
 create 'testtable2_cf_country', "cf"

 disable 'testtable2'
 alter 'testtable2', METHOD => 'table_att', 'coprocessor' => 'hdfs://node1:8020/hbase_cp/libHbaseCoprocessor.jar|tthbase.coprocessor.PhysicalDeletionInCompaction|1001|arg1=1,arg2=2'
 alter 'testtable2', METHOD => 'table_att', 'secondaryIndex$1' => 'cf|country'
 enable 'testtable2'

 put 'testtable2', "key1", 'cf:country', "v1", 101
 flush 'testtable2' #hbase compaction ignores all data from memstore.
 put 'testtable2', "key1", 'cf:country', "v2", 102
 flush 'testtable2' #hbase compaction ignores all data from memstore.
 major_compact 'testtable2'
 get 'testtable2', 'key1', {COLUMN => 'cf:country', VERSIONS => 4} #1 version
 describe 'testtable2'
 */

import ict.ictbase.commons.global.GlobalHTableGetByIndex;
import ict.ictbase.util.HIndexConstantsAndUtils;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class IssueCompaction {
	public static final Log LOG = LogFactory
			.getLog(HIndexConstantsAndUtils.class);
	static byte[] columnFamily = Bytes.toBytes("cf");
	static String indexedColumnName = "country";
	static byte[] indexTableName = null;
	static String coprocessorJarLoc = "hdfs://data8:9000/jar/ICTBase-1-0.0.1-SNAPSHOT.jar";

	static GlobalHTableGetByIndex htable = null;
	static Configuration conf = null;
	static byte[] rowKey = Bytes.toBytes("key1");

	static void fakedCreateTable(byte[] dataTableName) throws IOException {
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(dataTableName);
		TableName indexTN = TableName.valueOf(indexTableName);
		if (admin.tableExists(tn)) {
			admin.disableTable(tn);
			admin.deleteTable(tn);
		}
		if (admin.tableExists(indexTN)) {
			admin.disableTable(indexTN);
			admin.deleteTable(indexTN);
		}

		HIndexConstantsAndUtils.createAndConfigBaseTable(conf, dataTableName,
				columnFamily, new String[] { indexedColumnName });
		// create index table
		HIndexConstantsAndUtils.createAndConfigIndexTable(conf, indexTableName,
				columnFamily);
		htable = new GlobalHTableGetByIndex(conf, dataTableName);
	}

	static void fakedLoadData(byte[] dataTableName) throws IOException {
		// load data
//		for(int i=0;i<10;i++){
//			Put p = new Put(rowKey);
//			p.addColumn(columnFamily, Bytes.toBytes(indexedColumnName), 100+i,
//					Bytes.toBytes("v"+i));
//			htable.put(p);
//		}
		
		// put value1
		Put p = new Put(rowKey);
		p.addColumn(columnFamily, Bytes.toBytes(indexedColumnName), 101,
				Bytes.toBytes("v1"));
		htable.put(p);

		// put value2
		p = new Put(rowKey);
		p.addColumn(columnFamily, Bytes.toBytes(indexedColumnName), 100,
				Bytes.toBytes("v2"));
		htable.put(p);
		
		// put value3
		p = new Put(rowKey);
		p.addColumn(columnFamily, Bytes.toBytes(indexedColumnName), 18,
				Bytes.toBytes("v3"));
		htable.put(p);
		
		// put value4
		p = new Put(rowKey);
		p.addColumn(columnFamily, Bytes.toBytes(indexedColumnName), 99,
				Bytes.toBytes("v4"));
		htable.put(p);
		
		// put value5
		p = new Put(rowKey);
		p.addColumn(columnFamily, Bytes.toBytes(indexedColumnName), 98,
				Bytes.toBytes("v5"));
		htable.put(p);				
		
	}

	public static void issueMajorCompactionAsynchronously(byte[] dataTableName) {
		try {
			Connection con = ConnectionFactory.createConnection(conf);
			Admin admin = con.getAdmin();
			TableName tn = TableName.valueOf(dataTableName);
			admin.flush(tn);
			admin.majorCompact(tn);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void fakedTest(byte[] dataTableName) {
		try {
			// get th'm all
			Get g = null;
			Result res = null;
			g = new Get(rowKey);
			g.addColumn(columnFamily, Bytes.toBytes(indexedColumnName));
			g.setMaxVersions();
			res = htable.get(g);
			List<Cell> rl = res.getColumnCells(columnFamily,
					Bytes.toBytes(indexedColumnName));
			LOG.debug("TTDEBUG: " + rl.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void fakedTeardown(byte[] dataTableName) {
		try {
			// teardown
			if (htable != null) {
				htable.close();
			}
			HIndexConstantsAndUtils.deleteTable(conf, indexTableName);
			HIndexConstantsAndUtils.deleteTable(conf, dataTableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// if(args == null || args.length != 2){
		// System.out.println("format: 0.tablename 1.[yes|no]if_load_coprocessor_physical_deletion");
		// System.out.println("example: java tthbase.util.IssueMajorCompactionAsynchronously testtable yes");
		// return;
		// }
		// byte[] dataTableName = Bytes.toBytes(args[0]);
		// boolean loadCoprocessor = "yes".equals(args[1]);
		// System.out.println("TTDEBUG tablename=" +
		// Bytes.toString(dataTableName) + ", if2loadcoprocessor=" +
		// loadCoprocessor);

		String tableNameStr = "testtable_1";
		byte[] dataTableName = Bytes.toBytes(tableNameStr);
		boolean loadCoprocessor = true;
		conf = HBaseConfiguration.create();
		
//		indexTableName = HIndexConstantsAndUtils
//				.generateIndexTableName(dataTableName, columnFamily,
//						Bytes.toBytes(indexedColumnName));
//		try {
//			fakedCreateTable(dataTableName);
//			
//			if (loadCoprocessor) {
//				System.out.println("come in ");
//				HIndexConstantsAndUtils.updateCoprocessor(conf, dataTableName,
//						1, true, coprocessorJarLoc,
//						"ict.ictbase.coprocessor.PhysicalDeletionInCompaction");
//				HIndexConstantsAndUtils.updateCoprocessor(conf,
//						htable.getTableName(), 2, true, coprocessorJarLoc,
//						"ict.ictbase.coprocessor.IndexObserverwReadRepair");
//
//			} else {
//				HIndexConstantsAndUtils.updateCoprocessor(conf, dataTableName,
//						1, false, null, null);
//			}
//			fakedLoadData(dataTableName);
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
		
		issueMajorCompactionAsynchronously(dataTableName);
	}
}
