package ict.ictbase.util.local;

import ict.ictbase.commons.local.LocalHTableUpdateIndexByPut;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;

public class LocalQueueUtil{

	private LinkedBlockingQueue<Put> tablePutsQueue = null; 
	private ExecutorService executor = null;
	private LocalHTableUpdateIndexByPut dataTableWithLocalIndexes = null;
	private Put tempPut = null;
	private boolean initialized = false;
	private String startKey;
	private Region region;
	
	public static SyncRepairIndexCallable call;
	
	public static IncrementingEnvironmentEdge IEE;
	
	public LocalQueueUtil(LocalHTableUpdateIndexByPut DTWithIndexes,String startKey,Region region) {
		if(initialized ==false){
			executor = Executors.newSingleThreadExecutor();
			tablePutsQueue = new LinkedBlockingQueue<Put>();
			this.dataTableWithLocalIndexes=DTWithIndexes;
//			System.out.println("********* initialized ==false");
//			IEE = new IncrementingEnvironmentEdge();
			call = new SyncRepairIndexCallable();
			executor.submit(call);
			this.startKey = startKey;
			this.region = region;
		}
	}

//	public long getNowTime(){
//		return IEE.currentTime();
//	}
	
	public void addTablePutQueueMap(Put put) {
		tablePutsQueue.add(put);
//		executor.submit(call);
	}

	
	class SyncRepairIndexCallable implements Callable<Void> {

		public Void call() throws Exception {
			while(true){
				tempPut = tablePutsQueue.take();
				dataTableWithLocalIndexes.readBaseAndDeleteOld(tempPut,startKey,region);
//				dataTableWithLocalIndexes.insertNewToIndexes(tempPut,startKey,region);
			}
		}
	}

}
