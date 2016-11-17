package ict.ictbase.commons.local;
import ict.ictbase.commons.MaterializeIndex;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class MaterializeLocalIndexByCompositeRowkey implements MaterializeIndex {
  


	@Override
	public Map<byte[], List<byte[]>> getByIndexByRange(HTable indexTable,
			byte[] valueStart, byte[] valueStop) throws IOException {
		return null;
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
	public void deleteFromIndex(HTable indexTable, byte[] dataValue,
			byte[] dataKey) throws IOException {
		
	}

	@Override
	public void putToIndex(HTable indexTable, byte[] dataValue, byte[] dataKey)
			throws IOException {
		
	}
}

class IndexStorageFormat {
	    static final String INDEX_ROWKEY_DELIMITER = "/";
	    static final public byte[] INDEXTABLE_COLUMNFAMILY = Bytes.toBytes("INDEX_CF"); //be consistent with column_family_name in weblog_cf_country (in current preloaded dataset)
	    static final public byte[] INDEXTABLE_SPACEHOLDER = Bytes.toBytes("EMPTY");

	    static String[] parseIndexRowkey(byte[] indexRowkey){
	        return Bytes.toString(indexRowkey).split(INDEX_ROWKEY_DELIMITER);
	    }

	    static byte[] generateIndexRowkey(byte[] dataKey, byte[] dataValue){
	        return Bytes.toBytes(Bytes.toString(dataValue) + INDEX_ROWKEY_DELIMITER + Bytes.toString(dataKey));
	    }
	}

