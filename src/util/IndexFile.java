package util;

import helper.Helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import config.Config;

public class IndexFile {

	private String path;
	private String overFlowPath;
	private Integer nextPointer;
	private String dataType;
	private Integer columnLength;
	private Integer headerLength = 12;
	private Integer numberOfBuckets = 4;
	private Integer numberOfEntriesInBucket = Bucket.numberOfEntriesInBucket;

	public OverflowFile oFile; 

	// Intermediate Offset values
	public long offsetHeaderLength = 0;
	public long offsetColumnLength ;
	public long offsetNextPtr ;
	public long offsetEndOfHeader;
	public long currentFileOffset;

	public IndexFile (String path, String overflowPath, String datatype){
		this.path = path;
		this.overFlowPath = overflowPath;
		this.oFile = new OverflowFile(this.overFlowPath);
		this.nextPointer = 0;
		this.dataType = datatype;
		this.columnLength = Integer.parseInt(datatype.substring(1));
	}

	public Integer sizeOfBucket(){
		/* 
		 * Integer - maxSize + currentSize + Long - OffsetPointer + size of the 2D Object Data array 
		 */
		return (4 + 4 + 8 +(this.numberOfEntriesInBucket * (this.columnLength + 8)));
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
	public void writeToIndexFile(Object data, long ptr){
		Integer destinationBucketNumber = this.getHash( data);
		insertIntoDestinationBucket(destinationBucketNumber, data, ptr);
	}


	private void insertIntoDestinationBucket(Integer destinationBucketNumber,
			Object data, long ptr) {
		/*
		 *  ------ DONE -----
		 *  GOTO nth bucket using formula - 
		 *  this.currentFileOffset + (Size of each record * n);
		 *  
		 *  READ the entire bucket into memory, check if space exists.
		 *  TODO
		 *  If yes, then write to memory.
		 *   else, READ the overflow bucket into memory,
		 *  	check if space exists.
		 */

		Long destinationOffset = this.currentFileOffset + ((destinationBucketNumber)*sizeOfBucket());

		Bucket d = new Bucket(this.numberOfEntriesInBucket, null); 
		d = d.readBucketFromFile(path, destinationOffset, this.dataType);
		boolean writtenToBucket = false;
		if (d.writeInfoToBucket(data, ptr)){
			// Successfully entered into the same bucket
			if (Config.DEBUG) System.out.println("Successfully entered into the same bucket");

		}else {
			/*	No space in the current bucket.
			 *	Create a new bucket in the overflow file.
			 *	write the data to the new bucket.
			 *	
			 *  TODO Overflow logic. 
			 */
			if (Config.DEBUG) System.out.println("Overflow has occurred");

			Bucket nextBucket = d;
			Long startAddress ;
			
			while ((startAddress = nextBucket.getOverflowOffset()) != null){
				nextBucket = nextBucket.readBucketFromFile(overFlowPath, startAddress, this.dataType);
				if ( (writtenToBucket = nextBucket.writeInfoToBucket(data, ptr))){
					nextBucket.writeBucketToFile(this.overFlowPath,startAddress , this.dataType);
					break;
				}
				else continue;
			}
			if (!writtenToBucket){
				Bucket overFlow;
				/*
				 * Check if the Bucket exists in the free bucket list.
				 */
				if ((overFlow = Bucket.freeBuckets.get(0))!= null){
					overFlow.setCurrentSize(0);
					overFlow.setOverflowOffset(null);
				}else
					overFlow = new Bucket(this.numberOfEntriesInBucket, null);
				nextBucket.setOverflowOffset(this.oFile.currentFileOffset);
				overFlow.writeInfoToBucket(data, ptr);
				overFlow.writeBucketToFile(this.overFlowPath, this.oFile.currentFileOffset, this.dataType);
			}
		}
		System.out.println(destinationBucketNumber + "    "+ d);
		
		d.writeBucketToFile(path, destinationOffset, dataType);


	}


	public Integer getHash(Object data){
		/*
		 * Compute the hash value of the data.
		 */

		// TODO divide hashcode with the appropriate
		// function - so that the value lies within the correct 
		// set of buckets.

		Integer b = data.hashCode() % this.numberOfBuckets;
		if (b < this.nextPointer)
			b = data.hashCode() % (2*this.numberOfBuckets);
		return b; 

	}
	
	/*
	 * Write initial buckets to the 
	 * Index file
	 */
	public void writeInitialBucketsToFile (){
		
		Bucket initial ;
		long offsetForNewBucket = this.currentFileOffset;
		for (int i = 0; i< this.numberOfBuckets; i++){
			initial = new Bucket(numberOfEntriesInBucket, null);
			initial.writeData();
			initial.writeBucketToFile(this.path, offsetForNewBucket, this.dataType);
			offsetForNewBucket += sizeOfBucket();
		}
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
