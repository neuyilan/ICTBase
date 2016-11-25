package ict.ictbase.util;

import ict.ictbase.commons.global.GlobalHTableUpdateIndexByPut;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.client.Put;

public class QueueUtil {

	private static Map<GlobalHTableUpdateIndexByPut, LinkedBlockingQueue<Put>> tablePutsQueueMap = null; //new HashMap<HTableUpdateIndexByPut, LinkedBlockingQueue<Put>>();
	private ExecutorService executor = null;
	private GlobalHTableUpdateIndexByPut dataTableWithIndexes = null;
	private LinkedBlockingQueue<Put> putsQueue = null;
	private Put tempPut = null;

//	public ExecutorService getExecutor() {
//		if (executor == null) {
//			executor = Executors.newCachedThreadPool();
//		}
//		return executor;
//	}

	public QueueUtil(){
		SyncRepairIndexCallable call = new SyncRepairIndexCallable();
		SyncFutureTask syncFT = new SyncFutureTask(call);
		executor.submit(syncFT);
	}
	
	

	public void addTablePutQueueMap(
			GlobalHTableUpdateIndexByPut dataTableWithIndexes, Put put) {
		LinkedBlockingQueue<Put> tmpPutQueue = null;
		if (tablePutsQueueMap.containsKey(dataTableWithIndexes)) {
			tmpPutQueue = tablePutsQueueMap.get(dataTableWithIndexes);
		}else{
			tmpPutQueue = new LinkedBlockingQueue<Put>();
		}
		tmpPutQueue.offer(put);
		tablePutsQueueMap.put(dataTableWithIndexes, tmpPutQueue);
	}
	
	
	class SyncFutureTask extends FutureTask<Void>{

		public SyncFutureTask(Callable<Void> callable) {
			super(callable);
			System.out.println("********* come in the SyncFutureTask constructor method ");
		}
		protected void done(){
			try{
				System.out.println("********: "+get()+" thread have been completed");
			}catch(ExecutionException e){
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	
	class SyncRepairIndexCallable implements Callable<Void>{
		
		public Void call() throws Exception {
			System.out.println("********* come in the SyncRepairIndexCallable call method ");
			for (Iterator<Map.Entry<GlobalHTableUpdateIndexByPut, LinkedBlockingQueue<Put>>> it = tablePutsQueueMap
					.entrySet().iterator(); it.hasNext();) {
				Map.Entry<GlobalHTableUpdateIndexByPut, LinkedBlockingQueue<Put>> entry = it
						.next();
				dataTableWithIndexes = entry.getKey();
				putsQueue = entry.getValue();
				while (!putsQueue.isEmpty()) {
					tempPut = putsQueue.poll();
					dataTableWithIndexes.readBaseAndDeleteOld(tempPut);
					dataTableWithIndexes.insertNewToIndexes(tempPut);
				}
				it.remove();
			}
			return null;
		}
		
	}

}
