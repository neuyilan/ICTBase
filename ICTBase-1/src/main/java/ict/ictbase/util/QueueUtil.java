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
		if (executor == null) {
			System.out.println("******************  coming the QueueUtil init method executor == null");
			executor = Executors.newCachedThreadPool();
		}
		System.out.println("******************  coming the QueueUtil init method");
		if(tablePutsQueueMap ==null){
			System.out.println("******************  coming the QueueUtil init method tablePutsQueueMap == null");
			tablePutsQueueMap = new HashMap<GlobalHTableUpdateIndexByPut, LinkedBlockingQueue<Put>>();
		}
		Callable<Void> c = new Task();
		MyFutureTask ft = new MyFutureTask(c);
		executor.submit(ft);
	}
	
	
//	public void asyncMaintainIndex() {
//		System.out.println("$$$$$$$$$$$$$$$$$:come in asyncMaintainIndex");
//		Callable<Void> task = new Callable<Void>() {
//			public Void call() throws Exception {
//				for (Iterator<Map.Entry<HTableUpdateIndexByPut, LinkedBlockingQueue<Put>>> it = tablePutsQueueMap
//						.entrySet().iterator(); it.hasNext();) {
//					Map.Entry<HTableUpdateIndexByPut, LinkedBlockingQueue<Put>> entry = it
//							.next();
//					dataTableWithIndexes = entry.getKey();
//					putsQueue = entry.getValue();
//					System.out.println("$$$$$$$$$$$$$$$$$:putsQueue.size :"+putsQueue.size());
//					while (!putsQueue.isEmpty()) {
//						System.out.println("$$$$$$$$$$$$$$$$$:putsQueue is not empty");
//						tempPut = putsQueue.poll();
//						dataTableWithIndexes.readBaseAndDeleteOld(tempPut);
//						dataTableWithIndexes.insertNewToIndexes(tempPut);
//					}
//					it.remove();
//				}
//				return null;
//			}
//		};
//		executor.submit(task);
//	}
	

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
	
	class MyFutureTask extends FutureTask<Void>{

		public MyFutureTask(Callable<Void> callable) {
			super(callable);
		}
		protected void done(){
			try{
				System.out.println(get()+" thread have been completed");
			}catch(ExecutionException e){
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	
	class Task implements Callable<Void>{
		public Void call() throws Exception {
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
