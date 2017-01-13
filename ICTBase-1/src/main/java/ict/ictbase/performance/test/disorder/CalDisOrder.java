package ict.ictbase.performance.test.disorder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class CalDisOrder {
	
	public void readFile(String fileName){
		File file = new File(fileName);
		BufferedReader reader = null;
		
		try{
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line  = 1;
			while((tempString=reader.readLine())!=null ){
			}
		}catch(IOException e){
			
		}
		
		
		
		
	}
	
}
