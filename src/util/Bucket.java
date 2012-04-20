package util;

import helper.Helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import compare.Comparer;

public class Bucket {

	/*
	 * List of free buckets.
	 */
	public static ArrayList<Bucket> freeBuckets = new ArrayList<Bucket>();

	private Integer maxSize;
	private Integer currentSize;
	private Long overflowOffset;
	public Object [][] data;

	public static Integer numberOfEntriesInBucket = 4;

	public Bucket(Integer maxSize,Long overflowOffset){

		this.maxSize = maxSize;
		this.currentSize = 0;
		this.overflowOffset = overflowOffset;
		this.data = new Object [this.maxSize][2];
	}
	
	public void writeBucketToFile(String path, Long offset, String datatype){

		//		this.writeData(); // For testing purposes.
		RandomAccessFile raf ;
		Comparer comparer = new Comparer();

		try{
			raf = new RandomAccessFile(new File(path), "rw");
			raf.seek(offset);

			raf.write(Helper.toByta(this.maxSize));
			raf.write(Helper.toByta(this.currentSize));
			offset = raf.getFilePointer();

			for (int i = 0; i< this.maxSize ; i ++){
				// Use appropriate write method based on the datatype that the index file holds.
				comparer.compare_functions[comparer.mapper.indexOf(datatype)].writeAtOffset(raf, offset, this.data[i][0]+"", Integer.parseInt(datatype.substring(1)));
				offset += Integer.parseInt(datatype.substring(1));
				// Write the pointer , right after the 
				comparer.compare_functions[3].writeAtOffset(raf,offset,this.data[i][1]+"",8);
				offset += 8;
			}
			raf.close();
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	public boolean writeInfoToBucket(Object data, Long ptr){
		if (this.currentSize == this.maxSize){
			/*
			 * Bucket is full
			 * TODO Overflow logic
			 */
			return false;
		}else {
			/*
			 * There is space in the bucket.
			 * Insert the entry 
			 * return true
			 */
			this.data[this.currentSize][0] = data;
			this.data[this.currentSize][1] = ptr;
			this.currentSize ++;
			return true;
		}
		
		
	}
	// TODO Implementation pending
	public Bucket readBucketFromFile(String path, Long offset, String datatype){
		RandomAccessFile raf;
		Bucket temp = new Bucket(this.maxSize, (long) -1);
		byte[] tempData = new byte[4];
		long tempOffset = 0;
		Comparer comparer = new Comparer();
		try{
			raf = new RandomAccessFile(new File(path), "rw");
			raf.seek(offset);

			raf.read(tempData);
			temp.maxSize = Helper.toInt(tempData);
			raf.read(tempData);
			temp.currentSize = Helper.toInt(tempData);
			tempOffset = raf.getFilePointer();
			raf.close();

			for (int i = 0; i< this.maxSize ; i++){
				temp.data[i][0] = comparer.compare_functions[comparer.mapper.indexOf(datatype)].readString(path, (int) tempOffset, Integer.parseInt(datatype.substring(1)));
				tempOffset += Integer.parseInt(datatype.substring(1));
				
				// TODO RECTIFY ERROR
				temp.data[i][1] = comparer.compare_functions[3].readString(path, (int) tempOffset, 8);
				tempOffset += 8;
			}

			return temp;
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}

		return null;
	}

	/*
	 * Getter and Setters
	 */
	public Integer getMaxSize() {
		return maxSize;
	}
	public void setMaxSize(Integer maxSize) {
		this.maxSize = maxSize;
	}
	public Long getOverflowOffset() {
		return overflowOffset;
	}
	public void setOverflowOffset(Long overflowOffset) {
		this.overflowOffset = overflowOffset;
	}
	public Integer getCurrentSize() {
		return currentSize;
	}
	public void setCurrentSize(Integer currentSize) {
		this.currentSize = currentSize;
	}

	// Inserts dummy values into the data of the bucket 
	// so as to test the system.
	public void writeData() {
		// TODO Auto-generated method stub
		this.setOverflowOffset((long)-1);
		this.data [0][0]= -1; 	this.data[0][1] = -1;
		this.data [1][0]= -1; 	this.data[1][1] = -1;
		this.data [2][0]= -1;	this.data[2][1] = -1;
		this.data [3][0]= -1;	this.data[3][1] = -1;

	}

	public String toString(){
		String result = "";
		result += "MAXSIZE = "+ this.maxSize+ "\n";
		result += "DATA IS \n";
		for (int i = 0; i < this.maxSize; i++){
			for (int j = 0; j<2; j++)
				result += this.data[i][j] + "\t";
			result += "\n";
		}
		return result;
	}



}
