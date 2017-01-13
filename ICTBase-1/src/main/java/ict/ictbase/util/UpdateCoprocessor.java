//package ict.ictbase.util;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HConstants;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.KeepDeletedCells;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Admin;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.util.Pair;
//
///**
//1. update hbase coprocessor
//2. disable physical deletion in all column families where indexed columns are in.
//-KEEP_DELETED_CELLS = true
//-TTL = HConstants.FOREVER
//-maxVersion = Integer.MAX_VALUE
//*/
//
//public class UpdateCoprocessor {
//	public static final Log LOG = LogFactory.getLog(UpdateCoprocessor.class);
//    public static final byte[] FIXED_INDEX_CF = Bytes.toBytes("cf");
//    public static final String USAGE = "Create a table and associated index;\n " +
//            "Arguments:\n 1)zkserver 2)zkserver_port \n 3)table_name 4)list of cfs in table, in a single {},separated by ," +
//            "5) INDEX_CP_NAME 6) INDEX_CP_PATH 7) INDEX_CP_CLASS "+
//            "8)-[list of index columns in the format of cfName|colName]\n"+
//            "format: if INDEX_CP_CLASS contains null, any coprocessor will be unloaded\n" +
//            "***An example\n" +
//            "saba20.watson.ibm.com 2181 weblog {cf} coprocessor\\$1 hdfs://saba20.watson.ibm.com:8020/index-coprocessor-0.1.0.jar org.apache.hadoop.hbase.coprocessor.index.SyncSecondaryIndexObserver cf\\|country cf\\|ip";
//    
//    public static String KEEP_DELETED_CELLS = "KEEP_DELETED_CELLS";// = "coprocessor$1";
//    public static String INDEX_CP_NAME;// = "coprocessor$1";
//    public static String INDEX_CP_PATH;// = "hdfs://saba20.watson.ibm.com:8020/index-coprocessor-0.1.0.jar";
//    public static String INDEX_CP_CLASS;// = "org.apache.hadoop.hbase.coprocessor.index.SyncSecondaryIndexObserver";
//
//    public static String zkserver;//saba20.watson.ibm.com
//    public static String zkport;//2181
//    public static String dataTableName;//weblog
//    public static String cfList;
//    public static String [] indexItems;//{cf|country, cf|ip}
//    public static String cflist;
//    public static String [] cfs;
//    
//    public static void main(String[] args) throws IOException {
//        //six parameters to be given 
//        int bar = 8;
//        if(args.length<bar+1){
//            LOG.debug(USAGE);
//            return;
//        }
//        
//        zkserver = args[0];
//        zkport = args[1];    
//        dataTableName = args[2];
//        cflist = args[3];
//        INDEX_CP_NAME = args[4];
//        INDEX_CP_PATH = args[5];
//        INDEX_CP_CLASS = args[6];
//        
//        if(args.length>bar){
//            indexItems = new String[args.length-bar];
//            for(int i=bar;i<args.length;i++){
//                indexItems[i-bar]= args[i];                
//            }
//        }
//        
//        cfs = getColFamilys(cflist);
//        
//        LOG.debug("-----------Create table, deploy cp and create index definition-----");
//        Configuration conf = HBaseConfiguration.create();        
//        conf.set("hbase.zookeeper.quorum", zkserver);
//        conf.set("hbase.zookeeper.property.clientPort",zkport);   
//        LOG.debug("TTDEBUG: update coprocessor to " + INDEX_CP_NAME + "=>" + INDEX_CP_CLASS);
//        updateCoprocessor(conf, Bytes.toBytes(dataTableName));
//    }
//            
//    private static void updateCoprocessor(Configuration conf, byte[] dataTableName) throws IOException{
//    	Connection con = ConnectionFactory.createConnection(conf);
//    	Admin admin  = con.getAdmin();
//    	TableName tn = TableName.valueOf(dataTableName);
//        HTableDescriptor desc = admin.getTableDescriptor(tn);
//        admin.disableTable(tn);
//        LOG.debug("TTDEBUG: disable data table");
//        if(INDEX_CP_CLASS.contains("null")) {
//            desc.remove(Bytes.toBytes(INDEX_CP_NAME));
//        } else {
//            desc.setValue(INDEX_CP_NAME, INDEX_CP_PATH + "|" + INDEX_CP_CLASS + "|1001|arg1=1,arg2=2");
//        }
//
//        HColumnDescriptor descIndexCF = desc.getFamily(Bytes.toBytes("cf"));//TOREMOVE don't use cf, 
//        //KEEP_DELETED_CELLS => 'true'
//        descIndexCF.setKeepDeletedCells(KeepDeletedCells.TRUE);
//        descIndexCF.setTimeToLive(HConstants.FOREVER);
//        descIndexCF.setMaxVersions(Integer.MAX_VALUE);
//
//        admin.modifyTable(tn, desc);
//        LOG.debug("TTDEBUG: modify data table");
//        admin.enableTable(tn);
//        LOG.debug("TTDEBUG: enable data table");
//        HTableDescriptor descNew = admin.getTableDescriptor(tn);
//        //modify table is asynchronous, has to loop over to check
//        while (!desc.equals(descNew)){
//            LOG.debug("TTDEBUG: waiting for descriptor to change: from " + descNew + " to " + desc);
//            try {Thread.sleep(500);} catch(InterruptedException ex) {}
//            descNew = admin.getTableDescriptor(tn);
//        }
//    }
//
//    private static String[] getColFamilys(String cflist) {
//      //{cf1, cf2}
//      String t = cflist.substring(cflist.indexOf('{')+1, cflist.lastIndexOf('}'));
//      String temp[] = t.split(",");
//      for(int i=0;i<temp.length;i++) temp[i] = temp[i].trim();
//      return temp;
//    }
//
//
//    public static List<Pair<String,String>> getIndexCFAndColumn(HTableDescriptor htd) {        
//        List<Pair<String,String>> result = new ArrayList<Pair<String,String>>();
//        Pair<String,String> cfp = null;
//        int i = 1;
//        String index = null;
//        do {
//            index = htd.getValue(HIndexConstantsAndUtils.INDEX_INDICATOR + i);
//            if (index != null) {
//                String temp[] = index.split("\\"+HIndexConstantsAndUtils.INDEX_DELIMITOR);
//                cfp = new Pair<String, String>(temp[0],temp[1]);
//                if (cfp!=null) result.add(cfp);
//            }
//            i ++;
//        } while (index != null);
//        return result;
//    }
//}
