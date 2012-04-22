package util;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import config.Config;

public class CSVFile extends MyFile{

	public ArrayList<String> contents;

	public CSVFile (String path, ArrayList<String> contentFromHeap){
		this.path = path;
		contents = contentFromHeap;

	}
	public CSVFile (String path){
		this.path = path;
	}

	public void writeRecordToCSVFileUsingBufferedWriter(HeapFile hfile){		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)));
			int count = 0;
			// Write the schema to the file
			bw.write(hfile.schema+"\n");
			if (Config.DEBUG) System.out.println(hfile.schema);

			//	Fetch first record from the heap file.
			String currentRecord = hfile.getRecordFromHeapFile();
			count++;

			for (int i=0 ;i<hfile.numberOfRecords;i++){
				bw.write(currentRecord+"\n");
				//	Fetch subsequent records from the heap file.
				currentRecord = hfile.getRecordFromHeapFile();
				count++;
			}
			bw.close();
			if (Config.DEBUG) System.out.println(count + "Records written to file");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public String getRecordFromFile(BufferedReader br){
		try {
			return br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public String getSchemaFromFile(BufferedReader br){
		return getRecordFromFile(br);
	}

	public void getSchemaFromContents(){
		this.schema = this.contents.remove(0);
	}
	public void writeDataToFile(String string) {
		BufferedWriter bw;
		try {
			bw = new BufferedWriter(new FileWriter(path, true));
//			bw.newLine();
			bw.write(string);
			bw.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


}
