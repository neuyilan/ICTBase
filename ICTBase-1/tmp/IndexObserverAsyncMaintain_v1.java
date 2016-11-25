package ict.ictbase.coprocessor.global;

import ict.ictbase.commons.global.GlobalHTableUpdateIndexByPut;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

public class IndexObserverAsyncMaintain extends BasicIndexObserver {

	protected static Map<GlobalHTableUpdateIndexByPut, LinkedBlockingQueue<Put>> tablePutsQueueMap = null;
	protected ExecutorService executor = null;
	private boolean initQueue;

	private void syncInitQueue() {
		System.out.println("************ come in the syncInitQueue method");
		if (initQueue == false) {
			executor = Executors.newCachedThreadPool();
			tablePutsQueueMap = new HashMap<GlobalHTableUpdateIndexByPut, LinkedBlockingQueue<Put>>();
			initQueue = true;
			System.out
					.println("************ come in the syncInitQueue method and initQueue==false");
		}
	}

	@Override
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Put put, final WALEdit edit, final Durability durability)
			throws IOException {
		super.prePut(e, put, edit, durability);
		 this.addTablePutQueueMap(dataTableWithIndexes, put);
	}

	public void addTablePutQueueMap(
			GlobalHTableUpdateIndexByPut dataTableWithIndexes, Put put) {
		LinkedBlockingQueue<Put> tmpPutQueue = null;
		if (tablePutsQueueMap.containsKey(dataTableWithIndexes)) {
			tmpPutQueue = tablePutsQueueMap.get(dataTableWithIndexes);
		} else {
			tmpPutQueue = new LinkedBlockingQueue<Put>();
		}
		tmpPutQueue.offer(put);
		tablePutsQueueMap.put(dataTableWithIndexes, tmpPutQueue);

		SyncRepairIndexCallable call = new SyncRepairIndexCallable();
		SyncFutureTask syncFT = new SyncFutureTask(call);
		executor.submit(syncFT);
	}

	class SyncFutureTask extends FutureTask<Void> {

		public SyncFutureTask(Callable<Void> callable) {
			super(callable);
			System.out.println("********* come in the SyncFutureTask constructor method ");
		}

		protected void done() {
			try {
				System.out.println("********: " + get()
						+ " thread have been completed");
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	class SyncRepairIndexCallable implements Callable<Void> {
		private LinkedBlockingQueue<Put> putsQueue = null;
		private Put tempPut = null;

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
