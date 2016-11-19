package ict.ictbase.commons.local;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalMaterializeIndexByCompositeRowkey implements LocalMaterializeIndex {
  
	@Override
	public List<String> getByIndexByRange(HTable indexTable,
			byte[] valueStart, byte[] valueStop,byte[] columnFamily, byte[] columnName) throws IOException {
        Scan scan = new Scan();
        scan.setAttribute(IndexStorageFormat.SCAN_INDEX_FAMILIY, columnFamily);
        scan.setAttribute(IndexStorageFormat.SCAN_INDEX_QUALIFIER, columnName);
        scan.setAttribute(IndexStorageFormat.SCAN_START_VALUE, valueStart);
        scan.setAttribute(IndexStorageFormat.SCAN_STOP_VALUE, valueStop);
        
        //ResultScanner is for client-side scanning.
        List<String> toRet = new ArrayList<String>();
        ResultScanner rs = indexTable.getScanner(scan);
        try {
			Result r;
			while((r=rs.next())!=null){
				for(Cell cell : r.listCells()){
					System.out.println(String.format("come in row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
							Bytes.toString(CellUtil.cloneRow(cell)),
							Bytes.toString(CellUtil.cloneFamily(cell)),
							Bytes.toString(CellUtil.cloneQualifier(cell)),
							Bytes.toString(CellUtil.cloneValue(cell)),
							cell.getTimestamp()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
        rs.close();
        return toRet;
	}

	@Override
	public void putToIndex(HTable dataTable,String regionStartKey,byte [] columnFamily ,byte [] columnName,byte []  dataValue,byte []  dataKey)
			throws IOException {
		String indexRowkey = regionStartKey+"#"+Bytes.toString(columnFamily)+"#"+Bytes.toString(columnName)+"#"
							+Bytes.toString(dataValue)+"#"+Bytes.toString(dataKey);
        Put put2Index = new Put(Bytes.toBytes(indexRowkey));
        put2Index.addColumn(IndexStorageFormat.INDEXTABLE_COLUMNFAMILY, IndexStorageFormat.INDEXTABLE_SPACEHOLDER, IndexStorageFormat.INDEXTABLE_SPACEHOLDER);
        dataTable.put(put2Index);
	}

	
	@Override
	public void deleteFromIndex( HTable dataTable,String regionStartKey,byte [] columnFamily ,byte [] columnName,byte []  dataValue,byte []  dataKey) throws IOException {
		String indexRowkey = regionStartKey+"#"+Bytes.toString(columnFamily)+"#"+Bytes.toString(columnName)+"#"
				+Bytes.toString(dataValue)+"#"+Bytes.toString(dataKey);
        Delete del = new Delete(Bytes.toBytes(indexRowkey));
        dataTable.delete(del);
	}

}

class IndexStorageFormat {
	    static final public byte[] INDEXTABLE_COLUMNFAMILY = Bytes.toBytes("INDEX_CF"); //be consistent with column_family_name in weblog_cf_country (in current preloaded dataset)
	    static final public byte[] INDEXTABLE_SPACEHOLDER = Bytes.toBytes("EMPTY");
	    static final public String  SCAN_INDEX_FAMILIY="scan_index_family";
	    static final public String  SCAN_INDEX_QUALIFIER="scan_index_qualifier";
	    
	    static final public String  SCAN_START_VALUE="scan_start_value";
	    static final public String  SCAN_STOP_VALUE="scan_stop_value";
	}

