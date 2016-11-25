package ict.ictbase.util;

import ict.ictbase.commons.global.GlobalHTableUpdateIndexByPut;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;

public class QueueUtil{

	private LinkedBlockingQueue<Put> tablePutsQueue = null; 
	private ExecutorService executor = null;
	private GlobalHTableUpdateIndexByPut dataTableWithIndexes = null;
	private Put tempPut = null;
	private boolean initialized = false;
	
	public static SyncRepairIndexCallable call;
	
	public static IncrementingEnvironmentEdge IEE;
//	public static long uuid = 0;	
	
//	Lock lock = new ReentrantLock();
//	SyncFutureTask syncFT ;
		
	public QueueUtil(GlobalHTableUpdateIndexByPut DTWithIndexes) {
		if(initialized ==false){
//			executor = Executors.newCachedThreadPool();
			executor = //Executors.newFixedThreadPool(1);
					Executors.newSingleThreadExecutor();
			tablePutsQueue = new LinkedBlockingQueue<Put>();
			this.dataTableWithIndexes=DTWithIndexes;
			System.out.println("********* initialized ==false");
			IEE = new IncrementingEnvironmentEdge();
			
//			run();
			
			call = new SyncRepairIndexCallable();
////			syncFT = new SyncFutureTask(call);
//			executor.submit(call);
		}
		
//		if (executor == null) {
//			System.out.println("********* come in the QueueUtil method executor == null");
//			executor = Executors.newCachedThreadPool();
//		}
//		if (tablePutsQueue == null) {
//			System.out.println("********* come in the QueueUtil method tablePutsQueue == null");
//			tablePutsQueue = new LinkedBlockingQueue<Put>();
//		}
//		
//		if(dataTableWithIndexes == null){
//			System.out.println("********* come in the QueueUtil method dataTableWithIndexes == null");
//			this.dataTableWithIndexes=DTWithIndexes;
//		}
		
		System.out.println("********* come in the QueueUtil method");
		
		
	}

	
//	public long getUUID(){
//		synchronized(this){
//			return uuid++;
//		}
//	}
	
	public long getNowTime(){
		return IEE.currentTime();
	}
	
	
	
	public void addTablePutQueueMap(Put put) {
//		tablePutsQueue.offer(put);
		tablePutsQueue.add(put);
//		call = new SyncRepairIndexCallable();
//		syncFT = new SyncFutureTask(call);
		executor.submit(call);
	}

//	public void run() {
//		try{
//			while(true){
//				tempPut = tablePutsQueue.take();
//				System.out.println("***************** "+tempPut.toString());
//				dataTableWithIndexes.readBaseAndDeleteOld(tempPut);
//				dataTableWithIndexes.insertNewToIndexes(tempPut);
//			}
//		}catch (Exception ex){
//			ex.printStackTrace();
//		}
//	}

//	class SyncFutureTask extends FutureTask<Void> {
//
//		SyncFutureTask(Callable<Void> callable) {
//			super(callable);
//			System.out.println("********* come in the SyncFutureTask constructor method ");
//		}
//		protected void done() {
//			try {
//				System.out.println("********: " + get()+ " thread have been completed");
//			} catch (ExecutionException e) {
//				e.printStackTrace();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//
//	}

//	class SyncRepairIndexCallable implements Callable<Void> {
//
//		public Void call() throws Exception {
//			System.out.println("********* come in the SyncRepairIndexCallable call method ");
//			while (!tablePutsQueue.isEmpty()) {
//				System.out.println("********* come in the  tablePutsQueue.isEmpty()");
//				tempPut = tablePutsQueue.poll();
//				System.out.println("***************** "+tempPut.toString());
//				dataTableWithIndexes.readBaseAndDeleteOld(tempPut);
//				dataTableWithIndexes.insertNewToIndexes(tempPut);
//			}
//			return null;
//		}
//	}
	
	class SyncRepairIndexCallable implements Callable<Void> {

		public Void call() throws Exception {
			System.out.println("********* come in the SyncRepairIndexCallable call method ");
//			lock.lock();
			tempPut = tablePutsQueue.take();
			System.out.println("***************** "+tempPut.toString());
			dataTableWithIndexes.readBaseAndDeleteOld(tempPut);
			dataTableWithIndexes.insertNewToIndexes(tempPut);
//			lock.unlock();
			return null;
		}
	}
	
	
	
	

}
