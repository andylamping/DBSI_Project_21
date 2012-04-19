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
	private Long overflowOffset;
	public Object [][] data;


	public Bucket(Integer maxSize,Long overflowOffset){

		this.maxSize = maxSize;
		this.overflowOffset = overflowOffset;
		this.data = new Object [this.maxSize][2];
	}

	// TODO Implementation pending
	public void writeBucketToFile(String path, Long offset, String datatype){

		RandomAccessFile raf ;
		Comparer comparer = new Comparer();

		try{
			raf = new RandomAccessFile(new File(path), "rw");
			raf.seek(offset);

			raf.write(Helper.toByta(this.maxSize));
			raf.close();
			/*
			 * TODO write the data to the index file as 
			 * 		columnValue pointer columnValue pointer ... and so on.
			 */
			for (int i = 0; i< this.maxSize ; i ++){
				// Use appropriate write method based on the datatype that the index file holds.
				comparer.compare_functions[comparer.mapper.indexOf(datatype)].write(path, 0, this.data[i][0]+"", Integer.parseInt(datatype.substring(1)));
				// Write the pointer , right after the 
				comparer.compare_functions[3].write(path,0,this.data[i][1]+"",8);
			}
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	// TODO Implementation pending
	public Bucket readBucketFromFile(String path, Long offset, String datatype){
		RandomAccessFile raf;
		byte[] tempData = new byte[4];
		long tempOffset = 0;
		Comparer comparer = new Comparer();
		try{
			raf = new RandomAccessFile(new File(path), "rw");
			raf.seek(offset);

			raf.read(tempData);
			this.maxSize = Helper.toInt(tempData);
			tempOffset = raf.getFilePointer();
			raf.close();
			
			for (int i = 0; i< this.maxSize ; i++){
				this.data[i][0] = comparer.compare_functions[comparer.mapper.indexOf(datatype)].readString(path, (int) tempOffset, Integer.parseInt(datatype.substring(1)));
				tempOffset += Integer.parseInt(datatype.substring(1));
				this.data[i][1] = comparer.compare_functions[3].readString(path, (int) tempOffset, 8);
			}
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


}
