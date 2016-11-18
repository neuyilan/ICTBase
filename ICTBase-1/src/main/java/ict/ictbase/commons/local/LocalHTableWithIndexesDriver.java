package ict.ictbase.commons.local;

import ict.ictbase.util.HIndexConstantsAndUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalHTableWithIndexesDriver extends HTable {
	protected static int errorIndex = 0;
	protected Map<String, HTable> indexTables = null;
	protected LocalMaterializeIndex policyToMaterializeIndex = null;

	@SuppressWarnings("deprecation")
	public LocalHTableWithIndexesDriver(Configuration conf, byte[] tableName)
			throws IOException {
		 super(conf, tableName);
	        HTableDescriptor dataTableDesc = null; 
	        try {
	            dataTableDesc = getTableDescriptor();
	            //enable autoflush
	            setAutoFlush(true);
	        } catch (IOException e1) {
	            throw new RuntimeException("TTERROR_" + (errorIndex++) + ": " + e1.getMessage());
	        }

	        policyToMaterializeIndex = new LocalMaterializeIndexByCompositeRowkey(); //TOREMOVE
	        initIndexTables(dataTableDesc, conf);
	}
	
	
    public void initIndexTables(HTableDescriptor dataTableDesc, Configuration conf) {
        //initialize index table
        indexTables = new HashMap<String, HTable>();
        //scan through all indexed columns
        for (int indexNumber = 1; ; indexNumber++){
            String indexedColumn = dataTableDesc.getValue(HIndexConstantsAndUtils.INDEX_INDICATOR + indexNumber);
            if(indexedColumn == null){
                //no (further) index column, at current index
                break;
            } else {
                String[] names = indexedColumn.split("\\|");
                String indexedColumnFamilyName = names[0];
                String indexedColumnName = names[1]; 
                String indexTableName = dataTableDesc.getNameAsString() + "_" + indexedColumnFamilyName + "_" + indexedColumnName;
                indexTables.put(indexTableName, this);
            }
        }
    }
    
    /**
     * if user can use the index or not;
     * @param columnFamily
     * @param columnName
     * @return
     */
    public boolean isExistIndex(byte[] columnFamily, byte[] columnName){
    	String indexedColumnFamilyName = Bytes.toString(columnFamily);
        String indexedColumnName = Bytes.toString(columnName); 
    	String indexKey = null;
		try {
			indexKey = this.getTableDescriptor().getNameAsString() + "_" + indexedColumnFamilyName + "_" + indexedColumnName;
			if(indexTables.containsKey(indexKey)){
	    		return true;
	    	}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return false;
    }
    
    

	public void putToIndex(String regionStartKey,byte[] columnFamily, byte[] columnName,
			byte[] dataValue, byte[] dataKey) throws IOException {
		HTable indexTable = getIndexTable(columnFamily, columnName);
		System.out.println("****************putToIndex indexTable.getTableName() :"+Bytes.toString(indexTable.getTableName()));
		policyToMaterializeIndex.putToIndex(indexTable,regionStartKey,columnFamily,columnName, dataValue, dataKey);
	}
	
	
	
    public void deleteFromIndex(String regionStartKey,byte[] columnFamily, byte[] columnName,
			byte[] dataValue, byte[] dataKey) throws IOException {
    	System.out.println("****************deleteFromIndex:  "+Bytes.toString(columnFamily)+"\t"+Bytes.toString(columnName)+"\t"+
    			Bytes.toString(dataValue)+"\t"+Bytes.toString(dataKey));
        HTable indexTable = getIndexTable(columnFamily, columnName);
        policyToMaterializeIndex.deleteFromIndex(indexTable,regionStartKey,columnFamily ,columnName,dataValue,dataKey);
    } 
	
    /**
	@para, valueStop is exclusive!
    */
    protected List<String> internalGetByIndexByRange(byte[] columnFamily, byte[] columnName, byte[] valueStart, byte[] valueStop) throws IOException {
    	HTable indexTable = getIndexTable(columnFamily, columnName);
    	System.out.println("****************internalGetByIndexByRange indexTable.getTableName() :"+Bytes.toString(indexTable.getTableName()));
    	return policyToMaterializeIndex.getByIndexByRange(indexTable, valueStart, valueStop,columnFamily,columnName);
    }

	public HTable getIndexTable(byte[] columnFamily, byte[] columnName) {
		return this;
//		String dataTableName = Bytes.toString(this.getTableName());
//		Connection con;
//		HTable dataTable = null;
//		try {
//			con = ConnectionFactory.createConnection(this.getConfiguration());
//			TableName tableName = TableName.valueOf(dataTableName);
//			dataTable = (HTable) con.getTable(tableName);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		return dataTable;
	}
}
