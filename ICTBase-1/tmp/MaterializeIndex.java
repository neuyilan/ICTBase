//package ict.ictbase.commons;
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.hadoop.hbase.client.HTable;
//
//
//public interface MaterializeIndex {
//	
//	//global index
//    public Map<byte[], List<byte[]> > getByIndexByRange(HTable indexTable, byte[] valueStart, byte[] valueStop) throws IOException;
//    public void putToIndex(HTable indexTable, byte[] dataValue, byte[] dataKey) throws IOException;
//    public void deleteFromIndex(HTable indexTable, byte[] dataValue, byte[] dataKey) throws IOException;
//    
//    //local index
//	public void putToIndex(HTable dataTable,String regionStartKey,byte [] columnFamily ,byte [] columnName,byte []  dataValue,byte []  dataKey)throws IOException;
//	public void deleteFromIndex( HTable dataTable,String regionStartKey,byte [] columnFamily ,byte [] columnName,byte []  dataValue,byte []  dataKey) throws IOException;
//	public List<String> getByIndexByRange(HTable indexTable,byte[] valueStart, byte[] valueStop,byte[] columnFamily, byte[] columnName) throws IOException;
//}
