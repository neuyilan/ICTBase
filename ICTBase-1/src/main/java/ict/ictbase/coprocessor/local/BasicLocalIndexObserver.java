package ict.ictbase.coprocessor.local;

import ict.ictbase.commons.local.HTableUpdateLocalIndexByPut;
import ict.ictbase.coprocessor.LoggedObserver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

public class BasicLocalIndexObserver extends LoggedObserver {
    private boolean initialized;

    protected HTableUpdateLocalIndexByPut dataTableWithLocalIndexes = null;
    private void tryInitialize(HTableDescriptor desc) throws IOException {
        if(initialized == false) {
            synchronized(this) {
                if(initialized == false) {
                    Configuration conf = HBaseConfiguration.create();
                    System.out.println("%%%%%%%%%%%%%%%%%%%%%%: come in the local index coprocessor init method ");
                    dataTableWithLocalIndexes = new HTableUpdateLocalIndexByPut(conf, desc.getTableName().getName()); //this will make copy of data table instance.
                    initialized = true;
                }
            }
        }
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        setFunctionLevelLogging(false);
        initialized = false;
        super.start(e);
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put, final WALEdit edit, final Durability durability) throws IOException {
        super.prePut(e, put, edit, durability);
        tryInitialize(e.getEnvironment().getRegion().getTableDesc());
    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put, final WALEdit edit, final Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, final Durability durability) throws IOException {
        super.preDelete(e, delete, edit, durability);
        tryInitialize(e.getEnvironment().getRegion().getTableDesc());
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
      final Store store, final InternalScanner scanner, final ScanType scanType) throws IOException{
        InternalScanner toRet = super.preCompact(e, store, scanner, scanType);
        tryInitialize(e.getEnvironment().getRegion().getTableDesc());
        return toRet;
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> result) throws IOException {
        super.preGetOp(e, get, result);
        tryInitialize(e.getEnvironment().getRegion().getTableDesc());
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException { 
        super.stop(e);
        if(dataTableWithLocalIndexes != null){
        	dataTableWithLocalIndexes.close();
        }
    }
}
