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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GlobalPutAsyncDelete {
	private static String testTableName = "test_async_delete";
	private static String columnFamily = "cf";
	private static String indexedColumnName = "country";
	private static Configuration conf;
	private static String coprocessorJarLoc = "hdfs://data8:9000/jar/ICTBase-1-0.0.1-SNAPSHOT.jar";
	private static GlobalHTableGetByIndex htable;

	
	private static String startKeyStr = "a";
	private static String endKeyStr = "z";
	private static int numberOfRegions = 10;
	private static String INDEXTABLE_COLUMNFAMILY = "JUST_FOR_TEST_CF";
	
	public static void initTables(Configuration conf, String testTableName,
			String columnFamily, String indexedColumnName) throws Exception {
		Connection con = ConnectionFactory.createConnection(conf);
		Admin admin = con.getAdmin();
		TableName tn = TableName.valueOf(testTableName);

		if (admin.isTableAvailable(tn)) {
			HIndexConstantsAndUtils.deleteTable(conf,
					Bytes.toBytes(testTableName));
		}

		
//		HIndexConstantsAndUtils.createAndConfigBaseTable(conf,
//				Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily),
//				new String[] { indexedColumnName });
		
		
		HIndexConstantsAndUtils.createAndConfigAndSplitBaseTable(conf,
				Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily),Bytes.toBytes(INDEXTABLE_COLUMNFAMILY),
				new String[] { indexedColumnName },startKeyStr,endKeyStr,numberOfRegions);
		
		

		byte[] indexTableName = HIndexConstantsAndUtils.generateIndexTableName(
				Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily),
				Bytes.toBytes(indexedColumnName));

		TableName indexTN = TableName.valueOf(indexTableName);

		if (admin.isTableAvailable(indexTN)) {
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
				"ict.ictbase.coprocessor.global.GlobalIndexObserverAsyncMaintain");

	}

	public static void loadData() throws IOException {
		// load data
		String rowkeyStr = "key_async";
		byte[] rowKey = Bytes.toBytes(rowkeyStr);
		for (int i = 0; i < 20000; i++) {
			Put p = new Put(rowKey);
			long value = i;
			
//			long ts = System.currentTimeMillis();
//			p.addColumn(Bytes.toBytes(columnFamily),
//					Bytes.toBytes(indexedColumnName), ts,
//					Bytes.toBytes("va" + value));
//			p.setAttribute("put_time_version", Bytes.toBytes(ts));
			
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName),
					Bytes.toBytes("va" + value));
			
			
//			 Map<byte[], List<Cell>> familyMap = p.getFamilyCellMap();
			 
//			 for(Entry<byte[], List<Cell>> entry: familyMap.entrySet()){
//				 System.out.println(Bytes.toString(entry.getKey()));
//				 
//				 List<Cell> cells = entry.getValue();
//				 
//				 for(Cell cell:cells){
//					 System.out.println("*******"+Bytes.toString(CellUtil.cloneFamily(cell))+","+
//				 Bytes.toString(CellUtil.cloneQualifier(cell))+","+Bytes.toString(CellUtil.cloneValue(cell))+
//				 ","+Bytes.toString(CellUtil.cloneRow(cell)));
//				 }
//				 
//			 }
			
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
		htable = new GlobalHTableGetByIndex(conf, Bytes.toBytes(testTableName));
		initCoProcessors(conf, coprocessorJarLoc, htable);
//		loadData();

		
		loadData2MutilRegion();
		
		// getByIndex
//		htable.configPolicy(GlobalHTableGetByIndex.PLY_FASTREAD);
//		List<byte[]> res = htable.getByIndex(Bytes.toBytes(columnFamily),
//				Bytes.toBytes(indexedColumnName), Bytes.toBytes("v10"));
//		assert (res != null && res.size() != 0);
//		System.out.println("Result is " + Bytes.toString(res.get(0)));
		
	}
}