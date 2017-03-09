package ict.ictbase.commons.global;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

// specific to index materialization by composite index rowkey (i.e., indexRowKey=value/key)
public class GlobalMaterializeIndexByCompositeRowkey implements GlobalMaterializeIndex {
	
	//using scanner
    public List<String> getByIndexByRange(HTable indexTable, byte[] valueStart, byte[] valueStop) throws IOException {
        Scan scan = new Scan();
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL); //return rows that meet all filter conditions. (AND)
        fl.addFilter(new FirstKeyOnlyFilter());//return first instance of a row, then skip to next row. (avoiding rows of the same keys).
        fl.addFilter(new KeyOnlyFilter());// return only the key, not the value.
        assert valueStart != null;
        scan.setStartRow(valueStart);
        if (valueStop == null){ //point query
            Filter prefixFilter = new PrefixFilter(valueStart);
            fl.addFilter(prefixFilter);
        } else {
            scan.setStopRow(valueStop);
        }
        scan.setFilter(fl);
        
     // ResultScanner is for client-side scanning.
 		List<String> toRet = new ArrayList<String>();
 		ResultScanner rs = indexTable.getScanner(scan);
// 		try {
// 			Result r;
// 			r = rs.next();
 			
 			
 			for(Result r:rs){
 				
// 				for (Cell cell : r.listCells()) {
// 					System.out.println(String.format("come in row:%s,family:%s,qualifier:%s,value:%s,timestamp:%s",
// 											Bytes.toString(CellUtil
// 													.cloneRow(cell)),
// 											Bytes.toString(CellUtil
// 													.cloneFamily(cell)),
// 											Bytes.toString(CellUtil
// 													.cloneQualifier(cell)),
// 											Bytes.toString(CellUtil
// 													.cloneValue(cell)), cell
// 													.getTimestamp()));
// 				}
 				break;
 			}
// 		} catch (IOException e) {
// 			e.printStackTrace();
// 		}
 		rs.close();
 		return toRet;
    }

	
//  public Map<byte[], List<byte[]> > getByIndexByRange(HTable indexTable, byte[] valueStart, byte[] valueStop) throws IOException {
//  //read against index table
//  Scan scan = new Scan();
//  FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL); //return rows that meet all filter conditions. (AND)
//  fl.addFilter(new FirstKeyOnlyFilter());//return first instance of a row, then skip to next row. (avoiding rows of the same keys).
//  fl.addFilter(new KeyOnlyFilter());// return only the key, not the value.
//  assert valueStart != null;
//  scan.setStartRow(valueStart);
//  if (valueStop == null){ //point query
//      Filter prefixFilter = new PrefixFilter(valueStart);
//      fl.addFilter(prefixFilter);
//  } else {
//      scan.setStopRow(valueStop);
//  }
//  scan.setFilter(fl);
//
//  ResultScanner scanner = indexTable.getScanner(scan);
//  //ResultScanner is for client-side scanning.
//  Map<byte[], List<byte[]> > toRet = new HashMap<byte[], List<byte[]>>();
//  for (Result r : scanner) {
//      if(r.rawCells().length == 0) continue;
//      for (Cell cell : r.rawCells()) {
//          byte[] indexRowkey = CellUtil.cloneRow(cell);
//          String [] parsedIndexRowkey = IndexStorageFormat.parseIndexRowkey(indexRowkey);
//          byte[] dataValue = Bytes.toBytes(parsedIndexRowkey[0]);
//          byte[] dataKey = Bytes.toBytes(parsedIndexRowkey[1]);
//
//          if(toRet.get(dataValue) == null){
//              List<byte[]> results = new ArrayList<byte[]>();
//              results.add(dataKey);
//              toRet.put(dataValue, results);
//          } else {
//              toRet.get(dataValue).add(dataKey);
//          }
//      }
//  }
//  scanner.close();
//  return toRet;
//}

   
    
    public void putToIndex(HTable indexTable, byte[] dataValue, byte[] dataKey) throws IOException {
        byte[] indexRowkey = IndexStorageFormat.generateIndexRowkey(dataKey, dataValue);
        Put put2Index = new Put(indexRowkey);
        put2Index.addColumn(IndexStorageFormat.INDEXTABLE_COLUMNFAMILY, IndexStorageFormat.INDEXTABLE_SPACEHOLDER, IndexStorageFormat.INDEXTABLE_SPACEHOLDER);
        indexTable.put(put2Index);
    }

    public boolean deleteFromIndex(HTable indexTable, byte[] dataValue, byte[] dataKey) throws IOException {
        byte[] indexRowkey = IndexStorageFormat.generateIndexRowkey(dataKey, dataValue);
        
//        Get get = new Get(indexRowkey);
//        
//        Result r = indexTable.get(get);
//        
//        if(r.isEmpty()){
//        	return false;
//        }else{
//        	 Delete del = new Delete(indexRowkey);
//             //del.setTimestamp(timestamp);
//             indexTable.delete(del);
//        }
//        return true;
        
        /***********************************time break down*******************************/
        Delete del = new Delete(indexRowkey);
        //del.setTimestamp(timestamp);
        indexTable.delete(del);
        /***********************************time break down*******************************/
        return true;
    }
}

class IndexStorageFormat {
//TOREMOVE below is specific to implemention by composedIndexRowkey (rowkey=value/key)
    static final String INDEX_ROWKEY_DELIMITER = "/";

    static final public byte[] INDEXTABLE_COLUMNFAMILY = Bytes.toBytes("cf"); //be consistent with column_family_name in weblog_cf_country (in current preloaded dataset)
    static final public byte[] INDEXTABLE_SPACEHOLDER = Bytes.toBytes("EMPTY");

    static String[] parseIndexRowkey(byte[] indexRowkey){
        return Bytes.toString(indexRowkey).split(INDEX_ROWKEY_DELIMITER);
    }

    static byte[] generateIndexRowkey(byte[] dataKey, byte[] dataValue){
        return Bytes.toBytes(Bytes.toString(dataValue) + INDEX_ROWKEY_DELIMITER + Bytes.toString(dataKey));
    }
}
