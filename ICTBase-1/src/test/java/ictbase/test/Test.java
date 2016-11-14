package ictbase.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

public class Test {
	private static Map<String, LinkedBlockingQueue<String>> tablePutsQueueMap = new HashMap<String,LinkedBlockingQueue<String>>();


	public static void main(String args[]){
		LinkedBlockingQueue<String> tempPutQueue = new LinkedBlockingQueue<String>();
		tempPutQueue.offer("put1");
		tempPutQueue.offer("put2");
		tempPutQueue.offer("put3");
		tablePutsQueueMap.put("key1", tempPutQueue);
		
		LinkedBlockingQueue<String> tempPutQueue2 = new LinkedBlockingQueue<String>();
		tempPutQueue2.offer("put11");
		tempPutQueue2.offer("put21");
		tablePutsQueueMap.put("key2", tempPutQueue2);
//		
//		
		LinkedBlockingQueue<String> tmp=null;
		if(tablePutsQueueMap.containsKey("key1")){
			tmp= tablePutsQueueMap.get("key1");
		}
		tmp.offer("put4");
		
		System.out.println(tablePutsQueueMap.size()+" *********");
		for(Iterator<Map.Entry<String, LinkedBlockingQueue<String>>> it = tablePutsQueueMap.entrySet().iterator();it.hasNext();){
			Map.Entry<String, LinkedBlockingQueue<String>> entry = it.next();
			String key = entry.getKey();
			System.out.println(key);
			tmp= tablePutsQueueMap.get(key);
			while(!tmp.isEmpty()){
				System.out.println(tmp.poll());
			}
			it.remove();
		}
		System.out.println(tablePutsQueueMap.size()+" *********");
		
		
		
		
		
		
		
//		for(Entry<String, LinkedBlockingQueue<String>> entry: tablePutsQueueMap.entrySet()){
//			String key = entry.getKey();
//			System.out.println(key);
//			tmp= tablePutsQueueMap.get(key);
//			while(!tmp.isEmpty()){
//				System.out.println(tmp.poll());
//			}
//			tablePutsQueueMap.remove(key);
//		}
	}
}
