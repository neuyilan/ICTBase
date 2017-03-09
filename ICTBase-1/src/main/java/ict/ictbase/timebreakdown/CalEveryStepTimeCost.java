package ict.ictbase.timebreakdown;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CalEveryStepTimeCost {
	static String filePath = "/home/qhl/cluster/logs/result/time-break-down";
	static String rFileName = filePath + "/log.out";

	static String wFileName = filePath + "/result.csv";

	public static void main(String args[]) {
		File rfile = new File(rFileName);
		BufferedReader reader = null;

		File wfile = new File(wFileName);
		BufferedWriter writer = null;

		List<Long> resultList = new ArrayList<Long>();

		try {
			reader = new BufferedReader(new FileReader(rfile));
			writer = new BufferedWriter(new FileWriter(wfile));

			String line1 = null;
			String line2 = null;
			long start;
			long end;

			while ((line1 = reader.readLine()) != null) {
				start = Long.valueOf(line1.split(":")[1].trim());

				line2 = reader.readLine();
				end = Long.valueOf(line2.split(":")[1].trim());

				resultList.add(end - start);

			}
			long put_base = 0;
			long read_base = 0;
			long delete_index = 0;
			long put_index = 0;

			int count = 0;
			for (int i = 0; i < resultList.size(); i = i + 4) {
				put_base += resultList.get(i);
				read_base += resultList.get(i + 1);
				delete_index += resultList.get(i + 2);
				put_index += resultList.get(i + 3);
				count++;
			}
			System.out.println(count);
			writer.write(put_base / count + "\n");
			writer.write(read_base / count + "\n");
			writer.write(delete_index / count + "\n");
			writer.write(put_index / count + "\n");
			writer.write("------------------" + "\n");

			for (Long result : resultList) {
				writer.write(result + "\n");
			}
			writer.flush();
		} catch (IOException e) {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

}
