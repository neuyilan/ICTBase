package ict.ictbase.performance.test.time;

import ict.ictbase.commons.global.GlobalHTableGetByIndex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PerfoGlobalAsyncWriteTest {
	private static String testTableName = "testtable";
	private static String columnFamily = "cf";
	private static String indexedColumnName = "field0";
	private static Configuration conf;
	private static GlobalHTableGetByIndex htable;
	
	

	public static void loadData() throws IOException {
		String rowkeyStr = "dey_async";
		byte[] rowKey = Bytes.toBytes(rowkeyStr);
//		while(true){
//			Put p = new Put(rowKey);
//			p.addColumn(Bytes.toBytes(columnFamily),
//					Bytes.toBytes(indexedColumnName),
//					Bytes.toBytes("haha"));
//			htable.put(p);
//		}
		
		for(int i =0;i<100;i++){
			Put p = new Put(rowKey);
			p.addColumn(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName),
					Bytes.toBytes("haha"));
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
		htable = new GlobalHTableGetByIndex(conf, Bytes.toBytes(testTableName));
		long startTime  = System.currentTimeMillis();
		loadData();
		long endTime = System.currentTimeMillis();
		System.out.println("startTime: "+startTime+", endTime: "+endTime+", totalTime: "+(endTime-startTime));
	}
}
