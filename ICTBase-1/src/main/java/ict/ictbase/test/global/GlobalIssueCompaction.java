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
import java.util.Map;
import java.util.NavigableSet;

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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class GlobalIssueCompaction {
	public static final Log LOG = LogFactory
			.getLog(HIndexConstantsAndUtils.class);
	
	private static String coprocessorJarLoc = "hdfs://data8:9000/jar/ICTBase-1-0.0.1-SNAPSHOT.jar";
	
	static String testTableName = "testtable";
	static String columnFamily = "cf";
	static String indexedColumnName = "field0";
	static byte[] rowKey = Bytes.toBytes("g_key_compact");
	
	
	private static String startKeyStr = "0";
	private static String endKeyStr = "9999";
	private static int numberOfRegions = 20;
	private static String INDEXTABLE_COLUMNFAMILY = "JUST_FOR_TEST_CF";
	
	
	private static GlobalHTableGetByIndex htable = null;
	private static Configuration conf = null;
	

	static void initTables(Configuration conf, String testTableName,
			String columnFamily, String indexedColumnName) throws Exception{
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(testTableName);
		
		if (admin.isTableAvailable(tn)) {
			HIndexConstantsAndUtils.deleteTable(conf,
					Bytes.toBytes(testTableName));
		}
		
//		HIndexConstantsAndUtils.createAndConfigBaseTable(conf, Bytes.toBytes(testTableName),
//				Bytes.toBytes(columnFamily), new String[] { indexedColumnName });
		
		HIndexConstantsAndUtils.createAndConfigAndSplitBaseTable(conf,
				Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily),Bytes.toBytes(INDEXTABLE_COLUMNFAMILY),
				new String[] { indexedColumnName },startKeyStr,endKeyStr,numberOfRegions);
		
		byte[] indexTableName = HIndexConstantsAndUtils.generateIndexTableName(
				Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily),
				Bytes.toBytes(indexedColumnName));
		TableName indexTN = TableName.valueOf(indexTableName);
		
		if (admin.isTableAvailable(indexTN)) {
			HIndexConstantsAndUtils.deleteTable(conf,
					indexTableName);
		}
		
		HIndexConstantsAndUtils.createAndConfigIndexTable(conf, indexTableName,
				Bytes.toBytes(columnFamily));
		
		initIndexTableCoProcessors(conf,coprocessorJarLoc,indexTableName);
	}

	static void loadData() throws IOException {
		// load data
//		for(int i=0;i<22;i++){
//			Put p = new Put(rowKey);
//			p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(indexedColumnName), 100+i,
//					Bytes.toBytes("v"+i));
//			htable.put(p);
//		}
		
//		
//		for(int i=0;i<23;i++){
//			Put p = new Put(Bytes.toBytes("key_compact_"+i));
//			p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(indexedColumnName), 200+i,
//					Bytes.toBytes("v"+i*200));
//			htable.put(p);
//		}
		
	}
	
	
	public static void loadData2MutilRegion() throws IOException {
		for (int i = 0; i < 10000; i++) {
			rowKey = Bytes.toBytes(""+i);
			Put p = new Put(rowKey);
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName),
					Bytes.toBytes("va"+i));
			htable.put(p);
		}
	}
	
	public static void initCoProcessors(Configuration conf,
			String coprocessorJarLoc, GlobalHTableGetByIndex htable) throws IOException{
		int coprocessorIndex = 1;
		HIndexConstantsAndUtils.updateCoprocessor(conf, htable.getTableName(),
				coprocessorIndex++, true, coprocessorJarLoc,
				"ict.ictbase.coprocessor.global.GlobalPhysicalDeletionInCompaction");
		
		HIndexConstantsAndUtils.updateCoprocessor(conf, htable.getTableName(),
				coprocessorIndex++, true, coprocessorJarLoc,
				"ict.ictbase.coprocessor.global.GlobalIndexObserverJustPut");
	}

	
	public static void initIndexTableCoProcessors(Configuration conf,
			String coprocessorJarLoc, byte[] htable) throws Exception {
		int coprocessorIndex = 1;
		HIndexConstantsAndUtils.updateCoprocessor(conf, htable,
				coprocessorIndex++, true, coprocessorJarLoc,
				"ict.ictbase.coprocessor.global.GlobalIndexScanObserver");
	}
	
	
	public static void issueMajorCompactionAsynchronously() {
		try {
			Connection con = ConnectionFactory.createConnection(conf);
			Admin admin = con.getAdmin();
			TableName tn = TableName.valueOf(testTableName);
			admin.flush(tn);
			admin.majorCompact(tn);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void getTest(byte[] dataTableName) {
		try {
			Get g = null;
			Result res = null;
			g = new Get(rowKey);
			g.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(indexedColumnName));
			g.setMaxVersions();
			res = htable.get(g);
			List<Cell> rl = res.getColumnCells(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName));
			LOG.debug("TTDEBUG: " + rl.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void scanTest() throws IOException{
		Scan scan = new Scan();
		scan.setMaxVersions(Integer.MAX_VALUE);
		
		
		Connection con = ConnectionFactory.createConnection(conf);
		TableName tn = TableName.valueOf(testTableName);
		Table table = con.getTable(tn);
		ResultScanner rs = table.getScanner(scan);
		
		Result r = null;
		while((r=rs.next())!=null){
			for(Cell cell : r.rawCells()){
				System.out.println(String.format("pre row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
						Bytes.toString(CellUtil.cloneRow(cell)),
						Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)),
						Bytes.toString(CellUtil.cloneValue(cell)),
						cell.getTimestamp()));
			}
		}
		
	}
	
	
	public  static void deleteTest() throws IOException{
		Connection con = ConnectionFactory.createConnection(conf);
		TableName tn = TableName.valueOf(testTableName);
		
		String deleteRowKey="key_compact_3";
		Delete delete = new Delete(Bytes.toBytes(deleteRowKey));	
		Table table = con.getTable(tn);
		table.delete(delete);
	}


	public static void main(String[] args) throws Exception{

		conf = HBaseConfiguration.create();
////		
		initTables(conf, testTableName, columnFamily, indexedColumnName);
		htable = new GlobalHTableGetByIndex(conf, Bytes.toBytes(testTableName));
		initCoProcessors(conf, coprocessorJarLoc, htable);
//		loadData();
//		loadData2MutilRegion();
//		deleteTest();
		
		
//		issueMajorCompactionAsynchronously();
//		scanTest();
	}
}
