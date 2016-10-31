package ict.ictbase.commons;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;


interface MaterializeIndex {
    public Map<byte[], List<byte[]> > getByIndexByRange(HTable indexTable, byte[] valueStart, byte[] valueStop) throws IOException;

    public void putToIndex(HTable indexTable, byte[] dataValue, byte[] dataKey) throws IOException;

    public void deleteFromIndex(HTable indexTable, byte[] dataValue, byte[] dataKey) throws IOException;

}
