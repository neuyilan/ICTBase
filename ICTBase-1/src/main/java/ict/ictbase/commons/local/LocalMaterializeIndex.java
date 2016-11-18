package ict.ictbase.commons.local;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;


public interface LocalMaterializeIndex {
    //local index
	public void putToIndex(HTable dataTable,String regionStartKey,byte [] columnFamily ,byte [] columnName,byte []  dataValue,byte []  dataKey)throws IOException;
	public void deleteFromIndex( HTable dataTable,String regionStartKey,byte [] columnFamily ,byte [] columnName,byte []  dataValue,byte []  dataKey) throws IOException;
	public List<String> getByIndexByRange(HTable indexTable,byte[] valueStart, byte[] valueStop,byte[] columnFamily, byte[] columnName) throws IOException;
}
