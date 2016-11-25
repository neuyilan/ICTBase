package ict.ictbase.coprocessor.global;

import ict.ictbase.commons.global.GlobalHTableWithIndexesDriver;
import ict.ictbase.coprocessor.LoggedObserver;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
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
			System.out.println("***********come in the method filterKVs");
			System.out.println("***********results.size:"+results.size());
			
			
			byte[] buffer = null;
			int rowOffset = 0;
			int rowLength = 0;
			
////			/***************** here for delete *********/
//			byte[] preBuffer = null;
////			int preColumnFamilyOffset = 0;
////			int preColumnFamilyLength = 0;
////			int preColumnQualOffset = 0;
////			int preColumnQualLength = 0;
//			int preRowOffset = 0;
//			int preRowLength = 0;
//			/***************** here for delete *********/
			
			
			byte[] columnFamily = null;
			for (int i = 0; i < results.size(); ++i) {
				Cell kv = results.get(i);
				
				buffer = kv.getRowArray();
				rowOffset = kv.getRowOffset();
				rowLength = kv.getRowLength();
				
//				columnFamilyOffset = kv.getFamilyOffset();
//				columnFamilyLength = kv.getFamilyLength();
//				columnQualOffset = kv.getQualifierOffset();
//				columnQualLength = kv.getQualifierLength();
			
				System.out.println("**********buffer:"+Bytes.toString(buffer));
				columnFamily = CellUtil.cloneFamily(kv);
				if (GlobalPhysicalDeletionInCompaction.enableDebugLogging) {
					LoggedObserver.LOG.debug(new StringBuilder()
							.append("TTDEBUG: current KV=")
							.append((kv == null) ? "null_kv" : kv.toString())
							.append("; htable==null:")
							.append(this.htablewIndexes == null).toString());
				}
				if (preBuffer != null) {
					System.out.println("*******prebuffer:"+Bytes.toString(preBuffer));
					if (!(this.indexed
							.contains(new StringBuilder()
									.append(Bytes.toString(columnFamily))
									.append("|")
									.append(Bytes.toString(CellUtil
											.cloneQualifier(kv))).toString()))) {
						continue;
					}

//					boolean repeatRow = Bytes.compareTo(buffer,
//							columnFamilyOffset, columnFamilyLength, preBuffer,
//							preColumnFamilyOffset, preColumnFamilyLength) == 0;
//					System.out.println("********* repeatRow first: "+repeatRow);
//					repeatRow = (repeatRow)
//							&& (Bytes.compareTo(buffer, columnQualOffset,
//									columnQualLength, preBuffer,
//									preColumnQualOffset, preColumnQualLength) == 0);
//					System.out.println("********* repeatRow second: "+repeatRow);
					
					boolean repeatRow = Bytes.compareTo(buffer, rowOffset, rowLength, preBuffer, preRowOffset, preRowLength)==0;
					
					
					if(Bytes.toString(preBuffer).equals(Bytes.toString(buffer))){
						System.out.println("^^^^^^ equal ");
					}
					
					if(preBuffer==buffer){
						System.out.println("^^^^^^ == ");
					}
					
					if (repeatRow) {
						System.out.println("***********repeatRow ture");
						results.remove(i);
						--i;
						// this.htablewIndexes.deleteFromIndex(columnFamily,
						// CellUtil.cloneQualifier(kv), CellUtil.cloneValue(kv),
						// kv.getRow());
						this.htablewIndexes.deleteFromIndex(columnFamily,
								CellUtil.cloneQualifier(kv),
								CellUtil.cloneValue(kv), CellUtil.cloneRow(kv));
					}
				}else{
					System.out.println("********preBuffer is null ");
				}
//				System.out.println("%%*****preColumnFamilyOffset:"+preColumnFamilyOffset+",preColumnFamilyLength:"+
//						preColumnFamilyLength+",preColumnQualOffset"+preColumnQualOffset+",preColumnQualLength:"+preColumnQualLength);
//				
//				System.out.println("%%*****columnFamilyOffset:"+columnFamilyOffset+",columnFamilyLength:"+
//						columnFamilyLength+",columnQualOffset"+columnQualOffset+",columnQualLength:"+columnQualLength);
				System.out.println("%%*****preRowOffset:"+preRowOffset+",preRowLength:"+preRowLength);
				System.out.println("%%*****rowOffset:"+rowOffset+",rowLength:"+rowLength);
				preBuffer = buffer;
				preRowOffset = rowOffset;
				preRowLength = rowLength;
				
//				preColumnFamilyOffset = columnFamilyOffset;
//				preColumnFamilyLength = columnFamilyLength;
//				preColumnQualOffset = columnQualOffset;
//				preColumnQualLength = columnQualLength;
			}
		}

		public void close() {
			this.delegate.close();
		}

		public boolean next(List<Cell> results) throws IOException {
			boolean ifDone = this.delegate.next(results);
			/************************************************************/
			System.out.println("*************next_1*results.size():"
					+ results.size());
			for (int i = 0; i < results.size(); i++) {
				Cell cell = results.get(i);
				System.out.println("*************next_1:"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ","
						+ Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
						+ Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println("**************next_1:"
						+ results.get(i).toString());
			}
			/************************************************************/

			filterKVs(results);
			return ifDone;
		}

		public boolean next(List<Cell> results, ScannerContext scannerContext)
				throws IOException {
			boolean ifDone = this.delegate.next(results, scannerContext);
			/************************************************************/
			System.out.println("*************next_2*results.size():"
					+ results.size());
			for (int i = 0; i < results.size(); i++) {
				Cell cell = results.get(i);
				System.out.println("*************next_2:"
						+ Bytes.toString(CellUtil.cloneFamily(cell)) + ","
						+ Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
						+ Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println("**************next_2:"
						+ results.get(i).toString());
			}
			/************************************************************/
			filterKVs(results);
			return ifDone;
		}
	}
}