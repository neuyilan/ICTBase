package ict.ictbase.commons.local;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.Region;


public interface LocalMaterializeIndex {
    //local index
	public void putToIndex(Region region,String regionStartKey,byte [] columnFamily ,byte [] columnName,byte []  dataValue,byte []  dataKey)throws IOException;
	public boolean deleteFromIndex( Region region,String regionStartKey,byte [] columnFamily ,byte [] columnName,byte []  dataValue,byte []  dataKey) throws IOException;
	public List<String> getByIndexByRange(HTable indexTable,byte[] valueStart, byte[] valueStop,byte[] columnFamily, byte[] columnName) throws IOException;
}
