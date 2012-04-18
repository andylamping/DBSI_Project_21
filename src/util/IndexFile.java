package util;

import helper.Helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class IndexFile {
	
	private String path;
	private String overFlowPath;
	private Integer nextPointer;
	private Integer columnLength;
	private Integer headerLength = 12;
	
	// Intermediate Offset values
	public long offsetHeaderLength = 0;
	public long offsetColumnLength ;
	public long offsetNextPtr ;
	public long offsetEndOfHeader;
	public long currentFileOffset;
	
	public IndexFile (String path){
		this.path = path;
	}
	
	
	public void writeHeaderInformationToFile (){
		/*
		 * Write header information to the Index File
		 * Header Length -  always 12 bytes 	 - 	length 4 bytes (since we store Integer value)
		 * Column Length -	depends on the value - 	length 4 bytes (since we store Integer value)
		 * next Pointer  -	depends on the value - 	length 4 bytes (since we store Integer value)
		 */
		File f = new File(this.path);
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "rw");
			
			raf.write(Helper.toByta(this.headerLength));
			
			this.offsetColumnLength = this.currentFileOffset = raf.getFilePointer();
			raf.write(Helper.toByta(this.columnLength));
			
			this.offsetNextPtr = this.currentFileOffset = raf.getFilePointer();
			raf.write(Helper.toByta(this.nextPointer));
			
			this.offsetEndOfHeader = this.currentFileOffset = raf.getFilePointer();
			
			raf.close();
		} catch (FileNotFoundException e) {
			System.out.println("IndexFile - writing header information to file - " +
					"File Not Found!");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void getHeaderInformationFromFile(){
		File f = new File(this.path);
		
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(f, "rw");
			byte [] b = new byte [4];
			
			//Read Header Length 
			raf.seek(0);
			raf.read(b, 0, 4);
			this.headerLength = Helper.toInt(b);
			this.offsetHeaderLength = this.currentFileOffset = raf.getFilePointer();
			
			//Read Column Length 
			raf.read(b,0,4);
			this.columnLength = Helper.toInt(b);
			this.offsetColumnLength = this.currentFileOffset = raf.getFilePointer();
			
			//Read Next Pointer
			raf.read(b,0,4);
			this.columnLength = Helper.toInt(b);
			this.offsetNextPtr = this.currentFileOffset = raf.getFilePointer();
			
			raf.close();
			
		} catch (FileNotFoundException e) {
			System.out.println("IndexFile - Get Header Info from File - File not Found!");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/*
	 * TODO 
	 * Accept Data, get hashcode
	 */
	public void writeToIndexFile(Object data){
		
	}
	
	/* Getter and Setters */
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getOverFlowPath() {
		return overFlowPath;
	}
	public void setOverFlowPath(String overFlowPath) {
		this.overFlowPath = overFlowPath;
	}
	public Integer getNextPointer() {
		return nextPointer;
	}
	public void setNextPointer(Integer nextPointer) {
		this.nextPointer = nextPointer;
	}
	public Integer getColumnLength() {
		return columnLength;
	}
	public void setColumnLength(Integer columnLength) {
		this.columnLength = columnLength;
	}
	public Integer getHeaderLength() {
		return headerLength;
	}
	public void setHeaderLength(Integer headerLength) {
		this.headerLength = headerLength;
	}
	

}
