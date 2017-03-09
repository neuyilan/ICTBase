package ict.ictbase.performance.test.dataprepare;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class CalCount {
	static String filePath = "/home/qhl/cluster/logs/result/";
	static String consistenctMode = "local-async-insert";
	static String rFileName = filePath+consistenctMode+"/count.txt";
	
	static String wFileName = filePath+consistenctMode+"/"+consistenctMode+"_result.csv";
	
	public static void main(String args[]){
		File rfile = new File(rFileName);
		BufferedReader reader = null;
		
		File wfile = new File(wFileName);
		BufferedWriter writer = null;
		
		
		
		HashMap<Integer,Integer> map = new HashMap<Integer,Integer>();
		
		try{
			reader = new BufferedReader(new FileReader(rfile));
			writer = new BufferedWriter(new FileWriter(wfile));
			
			String tempString = null;
			Integer key ;
			while((tempString=reader.readLine())!=null ){
				key = Integer.valueOf(tempString);
				if(map.get(key)==null){
					map.put(key, 1);
				}else{
					map.put(key, map.get(key)+1);
				}
			}
			
			List<Map.Entry<Integer,Integer>> listData =  new ArrayList<Map.Entry<Integer,Integer>>(map.entrySet());
			CalCount.ByKeyIntComparator bv = new ByKeyIntComparator();
			Collections.sort(listData,bv);
			
			for(Entry<Integer,Integer> entry: listData){
				writer.write(entry.getKey()+","+entry.getValue()+"\n");
			}
			writer.flush();
		}catch(IOException e){
			if(reader!= null){
				try {
					reader.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			if(writer!= null){
				try {
					writer.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	
	static class ByKeyComparator implements Comparator<Map.Entry<String, Integer>>{
		public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
			return o1.getKey().toString().compareTo(o2.getKey().toString());
		}
	}
	
	
	static class ByValueComparator implements  Comparator<Map.Entry<String, Integer>>{
		public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
			return o1.getValue()-o2.getValue();
		}
	}
	
	static class ByKeyIntComparator implements Comparator<Map.Entry<Integer, Integer>>{
		public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
			return o1.getKey()-o2.getKey();
		}
	}
}
