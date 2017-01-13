package ict.ictbase.coprocessor.global;

import ict.ictbase.commons.global.GlobalHTableWithIndexesDriver;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class GlobalPhysicalDeletionInCompaction extends GlobalIndexBasicObserver {
	static boolean enableDebugLogging = false;

	
	@Override
	public InternalScanner preCompact(
			ObserverContext<RegionCoprocessorEnvironment> e, Store store,
			InternalScanner scanner, ScanType scanType) throws IOException {
		InternalScanner s = super.preCompact(e, store, scanner, scanType);
		if (scanner instanceof StoreScanner) {
			return new TTScannerWrapper((StoreScanner) scanner,
					this.dataTableWithIndexes);
		}
		return s;
	}
	
	@Override
	public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c,
			Store store, StoreFile resultFile) {
		super.postCompact(c, store, resultFile);

		if (resultFile == null) {
			return;
		}
		if (enableDebugLogging)
			LOG.debug("TTDEBUG: resultFile is " + resultFile);
	}

	static class TTScannerWrapper implements InternalScanner {
		StoreScanner delegate;
		GlobalHTableWithIndexesDriver htablewIndexes;
		Set<String> indexed;
		
		/***************** here for update *********/
		byte[] preBuffer = null;
		int preRowOffset = 0;
		int preRowLength = 0;
		/***************** here for update *********/
		
		
		public TTScannerWrapper(StoreScanner d, GlobalHTableWithIndexesDriver h)
				throws IOException {
			System.out.println("*******TTScannerWrapper  come in ");
			this.delegate = d;
			this.htablewIndexes = h;

			HTableDescriptor dataTableDesc = this.htablewIndexes
					.getTableDescriptor();
			this.indexed = new HashSet<String>();
			for (int index = 1;; ++index) {
				String fullpathOfIndexedcolumnInDatatable = dataTableDesc
						.getValue(new StringBuilder().append("secondaryIndex$")
								.append(index).toString());
				if (fullpathOfIndexedcolumnInDatatable == null) {
					return;
				}

				this.indexed.add(fullpathOfIndexedcolumnInDatatable);
			}
		}

		private void filterKVs(List<Cell> results) throws IOException {
			
			byte[] buffer = null;
			int rowOffset = 0;
			int rowLength = 0;
			
			byte[] columnFamily = null;
			for (int i = 0; i < results.size(); ++i) {
				Cell kv = results.get(i);
				
				buffer = kv.getRowArray();
				rowOffset = kv.getRowOffset();
				rowLength = kv.getRowLength();
			
				columnFamily = CellUtil.cloneFamily(kv);
				if (preBuffer != null) {
					if (!(this.indexed
							.contains(new StringBuilder()
									.append(Bytes.toString(columnFamily))
									.append("|")
									.append(Bytes.toString(CellUtil
											.cloneQualifier(kv))).toString()))) {
						continue;
					}

					boolean repeatRow = Bytes.compareTo(buffer, rowOffset, rowLength, preBuffer, preRowOffset, preRowLength)==0;
					
					
					if (repeatRow) {
						results.remove(i);
						--i;
						this.htablewIndexes.deleteFromIndex(columnFamily,
								CellUtil.cloneQualifier(kv),
								CellUtil.cloneValue(kv), CellUtil.cloneRow(kv));
					}
				}
				preBuffer = buffer;
				preRowOffset = rowOffset;
				preRowLength = rowLength;
			}
		}

		public void close() {
			this.delegate.close();
		}

		public boolean next(List<Cell> results) throws IOException {
			boolean ifDone = this.delegate.next(results);
			filterKVs(results);
			return ifDone;
		}

		public boolean next(List<Cell> results, ScannerContext scannerContext)
				throws IOException {
			boolean ifDone = this.delegate.next(results, scannerContext);
			filterKVs(results);
			return ifDone;
		}

		public void setScan(Scan scan) {
			// TODO Auto-generated method stub
			
		}

		public Scan getScan() {
			// TODO Auto-generated method stub
			return null;
		}
	}
}