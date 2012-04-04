package util;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class CSVFile extends MyFile{

	public ArrayList<String> contents;

	public CSVFile (String path, ArrayList<String> contentFromHeap){
		this.path = path;
		contents = new ArrayList<String>();
		if (contentFromHeap == null){
			this.getContentsFromFile();
			this.getSchemaFromContents();
			this.getSchemaArrayFromSchema();	
		}else {
			contents = contentFromHeap;
			this.getSchemaFromContents();
			this.writeRecordToFile();
		}

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

	private void writeRecordToFile() {
		try {
			RandomAccessFile raf = new RandomAccessFile(new File(this.path), "rw");
			raf.writeChars(this.schema);
			for (String s:this.contents){
				raf.writeChars(s);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private void getContentsFromFile() {
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

	private void getSchemaFromContents(){
		this.schema = this.contents.remove(0);
	}


}
