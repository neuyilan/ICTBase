package ict.ictbase.util.global;

import ict.ictbase.commons.global.GlobalHTableUpdateIndexByPut;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;

public class GlobalQueueUtil{

	private LinkedBlockingQueue<Put> tablePutsQueue = null; 
	private ExecutorService executor = null;
	private GlobalHTableUpdateIndexByPut dataTableWithIndexes = null;
	private Put tempPut = null;
	private boolean initialized = false;
	
	public static SyncRepairIndexCallable call;
	
//	public static IncrementingEnvironmentEdge IEE;
		
	public GlobalQueueUtil(GlobalHTableUpdateIndexByPut DTWithIndexes) {
		if(initialized ==false){
			executor = Executors.newSingleThreadExecutor();
			tablePutsQueue = new LinkedBlockingQueue<Put>();
			this.dataTableWithIndexes=DTWithIndexes;
//			System.out.println("********* initialized ==false");
//			IEE = new IncrementingEnvironmentEdge();
			call = new SyncRepairIndexCallable();
			executor.submit(call);
		}
	}

//	public long getNowTime(){
//		return IEE.currentTime();
//	}
	
	public void addTablePutQueueMap(Put put) {
		tablePutsQueue.add(put);
		
	}

	
	class SyncRepairIndexCallable implements Callable<Void> {

		public Void call() throws Exception {
			while(true){
				tempPut = tablePutsQueue.take();
				dataTableWithIndexes.readBaseAndDeleteOld(tempPut);
				//dataTableWithIndexes.insertNewToIndexes(tempPut);
			}
		}
		
	}

}
