package ict.ictbase.performance.test.time;

import ict.ictbase.commons.global.GlobalHTableGetByIndex;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

public class PerfoGlobalAsyncReadTest {
	private static String testTableName = "test_table";
	private static String columnFamily = "cf";
	private static String indexedColumnName = "field";
	private static Configuration conf;
	private static GlobalHTableGetByIndex htable;
	
	public static void main(String[] args) throws Exception {
		conf = HBaseConfiguration.create();
		if (args.length == 3) {
			testTableName = args[0];
			columnFamily = args[1];
			indexedColumnName = args[2];

		}
		htable = new GlobalHTableGetByIndex(conf, Bytes.toBytes(testTableName));
		List<String> res;
//		while(true){
////			System.out.println("****************come in res ");
//			htable.getByIndexData(Bytes.toBytes(columnFamily),
//					Bytes.toBytes(indexedColumnName), Bytes.toBytes("haha"));
//		}
		
		long startTime  = System.currentTimeMillis();
		for(int i =0;i<10000;i++){
			htable.getByIndexData(Bytes.toBytes(columnFamily),
					Bytes.toBytes(indexedColumnName), Bytes.toBytes("haha"));
		}
		long endTime = System.currentTimeMillis();
		System.out.println("startTime: "+startTime+", endTime: "+endTime+", totalTime: "+(endTime-startTime));
	}
}
