package util;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
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
		contents = new ArrayList<String>();
	}
	
	public CSVFile (String path, String output, int a){
		try {
			RandomAccessFile raf = new RandomAccessFile(new File(path), "rw");
			raf.writeChars(output);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void writeRecordToFile() {
		try {
			RandomAccessFile raf = new RandomAccessFile(new File(this.path), "rw");
			raf.writeUTF(this.schema);
			System.out.println(this.schema);
			for (String s:this.contents){
				raf.writeUTF(s);
				System.out.println(s);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void writeRecordToFileUsingBufferedWriter(){
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)));
			bw.write(schema);
			if (Config.DEBUG) System.out.println(schema);
			for (String s:this.contents){
				bw.write(s);
				if (Config.DEBUG) System.out.println(s);
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void getContentsFromFile() {
		// TODO Auto-generated method stub
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(this.path));
			String currentLine; 

			while ((currentLine = br.readLine())!= null){
				this.contents.add(currentLine);
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void getSchemaFromContents(){
		this.schema = this.contents.remove(0);
	}


}
