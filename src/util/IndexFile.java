package util;

import helper.Helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import config.Config;

public class IndexFile {

	private String path;
	private String overFlowPath;
	private Integer nextPointer;
	private String dataType;
	private Integer columnLength;
	private Integer headerLength = 12;
	private Integer numberOfBuckets = 4;
	private Integer startBuckets = 4;
	private Integer round = 1;
	private Integer splitting = 0;
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
		// * Integer - maxSize + currentSize + Long - OffsetPointer + size of the 2D Object Data array 
		 * Integer - maxSize + currentSize + number of Buckets +Long - OffsetPointer + size of the 2D Object Data array
		 */
	//	return (4 + 4 + 8 +(this.numberOfEntriesInBucket * (this.columnLength + 8)));
		return (4 + 4 + 4 +8 +(this.numberOfEntriesInBucket * (this.columnLength + 8)));
	}

	public void writeHeaderInformationToFile (){
		/*
		 * Write header information to the Index File
		 * Header Length -  always 12 bytes 	 - 	length 4 bytes (since we store Integer value)
		 * Column Length -	depends on the value - 	length 4 bytes (since we store Integer value)
		 * next Pointer  -	depends on the value - 	length 4 bytes (since we store Integer value)
		 */
		System.out.println("Writing headerinformation to index file");
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
		System.out.println("Inserting " + data + " into bucket:" + destinationBucketNumber);
		insertIntoDestinationBucket(destinationBucketNumber, data, ptr);
	}


	private void insertIntoDestinationBucket(Integer destinationBucketNumber,
			Object data, long ptr) {

		/*
		 * Goto nth bucket using the formula
		 * 
		 * READ the entire bucket into memory, check if space exists.
		 * if yes, then write to memory
		 * else,
		 * 		READ the overflow bucket into memory
		 * 		check if space exists.
		 */
		Long destinationOffset = this.headerLength + (long) ((destinationBucketNumber)*sizeOfBucket());

		Bucket d = new Bucket(this.numberOfEntriesInBucket, (long) -1);
		d = d.readBucketFromFile(path, destinationOffset, dataType);

		long overflowBucketStartAddress, lastOverflowBucketStartAddress = 0;
		boolean writtenToBucket = false;
		if (d.writeInfoToBucket(data, ptr)){
			if (Config.DEBUG) System.out.println("Successfully entered into the same bucket");
			writtenToBucket = true;
			System.out.println(destinationBucketNumber + "  " + d);	
			d.writeBucketToFile(path, destinationOffset, dataType);
		}
		else {

			if (Config.DEBUG) System.out.println("Overflow has occured!!!");
			Bucket currentBucket = d;
			Bucket overflowBucket= new Bucket(numberOfEntriesInBucket, (long)-1) ;
			long currentBucketStartAddress = destinationOffset;
			System.out.println("here");
			/*
			 * Iterate to empty bucket.
			 * Assumption - all Buckets are filled to the max.
			 */
			Iterate:
			while ((overflowBucketStartAddress = currentBucket.getOverflowOffset()) != -1){
				overflowBucket = overflowBucket.readBucketFromFile(overFlowPath, overflowBucketStartAddress, dataType);
				System.out.println("here!!");
				if (overflowBucket.writeInfoToBucket(data, ptr)){
					if (Config.DEBUG) System.out.println("Data entered to overflow bucket");
					writtenToBucket = true;
					overflowBucket.writeBucketToFile(overFlowPath, overflowBucketStartAddress, dataType);
					if (Config.DEBUG) System.out.println("Inserted into pre-existing bucket " + overflowBucket);
					
					if(this.splitting == 0){
						System.out.println("A split must occur.");
							this.split();}
					break Iterate;
				}
				currentBucket = overflowBucket;
				System.out.println("STUCK");
				currentBucketStartAddress = overflowBucketStartAddress;
			}
			
			if (!writtenToBucket){
				overflowBucket = new Bucket(numberOfEntriesInBucket, (long)-1);
				overflowBucket.writeData();
				overflowBucket.writeInfoToBucket(data, ptr);
				File f = new File(overFlowPath);
				long newOverflowBucketStartAddress = f.length();
				overflowBucket.writeBucketToFile(overFlowPath, newOverflowBucketStartAddress, dataType);
				if (Config.DEBUG) System.out.println("Inserted into new bucket " + overflowBucket);
				
				if (currentBucket == d){
					currentBucket.setOverflowOffset(newOverflowBucketStartAddress);
				    currentBucket.setNumberOfOverflowBuckets(currentBucket.getNumberOfOverflowBuckets()+1);
					currentBucket.writeBucketToFile(path, currentBucketStartAddress, dataType);
					System.out.println("A split must occur!");
					if(this.splitting == 0){
						System.out.println("A split must occur.");
							this.split();}
				}else{
					d.setNumberOfOverflowBuckets(d.getNumberOfOverflowBuckets() +1);
					d.writeBucketToFile(path, destinationOffset, dataType); 
					currentBucket.setOverflowOffset(newOverflowBucketStartAddress);
					currentBucket.writeBucketToFile(overFlowPath, currentBucketStartAddress, dataType);
					System.out.println("A split must occur!!");
					if(this.splitting == 0){
						System.out.println("A split must occur.");
							this.split();}
				}
			}
		}


	}
	
	public Integer getHash(Object data){
		/*
		 * Compute the hash value of the data.
		 */

		// TODO divide hashcode with the appropriate
		// function - so that the value lies within the correct 
		// set of buckets.
/**
		Integer b = data.hashCode() % this.numberOfBuckets;
		if (b < this.nextPointer){
			System.out.println("WEIRD!!!!!!!!");
			b = data.hashCode() % (2*this.numberOfBuckets);
		}
		return b; 
		**/
		System.out.println(data.getClass());
		String str = "hey";
		if(str.getClass() == data.getClass()){
			System.out.println("STRING");
		    str = data.toString();
			str = str.toLowerCase();
			Integer b = Math.abs(str.hashCode()) % this.numberOfBuckets;
			if(b < this.nextPointer)
				b = Math.abs(str.hashCode()) % (2 * this.numberOfBuckets);
			System.out.println(str + " !!! " + str.hashCode() + "  " + b);
			return b;
		}
		
		Integer b = Math.abs(data.hashCode()) % this.numberOfBuckets;
		if(b < this.nextPointer)
			b = Math.abs(data.hashCode()) % (2 * this.numberOfBuckets);
		System.out.println(data + " !!! " + data.hashCode() + "  " + b);
		return b;
	}

	/*
	 * Write initial buckets to the 
	 * Index file
	 */
	public void writeInitialBucketsToFile (){

		Bucket initial ;
		long offsetForNewBucket = this.currentFileOffset;
		System.out.println(this.dataType);
		for (int i = 0; i< this.numberOfBuckets; i++){
			initial = new Bucket(numberOfEntriesInBucket, (long)-1);
			initial.writeData();
			initial.writeBucketToFile(this.path, offsetForNewBucket, this.dataType);
			offsetForNewBucket += sizeOfBucket();
		}
	}
	/*
	 * Split the bucket n
	 */
	public void split(){
		this.splitting = 1;
		// increase number of buckets in index file
		
		Bucket freshBucket = new Bucket(numberOfEntriesInBucket, (long) -1);
		freshBucket.writeData();
		freshBucket.writeBucketToFile(this.path, this.headerLength + (long) this.numberOfBuckets * sizeOfBucket(), this.dataType);

		// set the offset for the current this.nextPointer bucket
		long offsetSplit = this.headerLength + this.nextPointer * sizeOfBucket();
		// create new bucket to copy that bucket
		Bucket splitBucket = new Bucket(numberOfEntriesInBucket, (long) -1);
		// copy that bucket to splitBucket
		splitBucket = splitBucket.readBucketFromFile(this.path, offsetSplit, this.dataType);
	//	System.out.println(" bucket to be split " + splitBucket.toString());
		int index = 0;
		// arrayList to store objects of the bucket and its overflow bucket
		ArrayList<Object> currentContents = new ArrayList<Object>();
		
		// get all data items from index bucket
		while ( index < splitBucket.getCurrentSize()){
			Object pluck = splitBucket.data[index][0];
				currentContents.add(pluck);
		//		System.out.println(pluck);
			index++;
			
		}
		 
	
		// get al
		int overFlowBucket1 = 0;
		System.out.println("OVERFLOWWWWSS" + splitBucket.getNumberOfOverflowBuckets());
		// traverse each overflow bucket
		while(overFlowBucket1 < splitBucket.getNumberOfOverflowBuckets()){
			System.out.println("enter");
			Bucket overflowBucket = splitBucket.readBucketFromFile(this.overFlowPath, splitBucket.getOverflowOffset() - overFlowBucket1 * sizeOfBucket(), this.dataType);

		
			index = 0;

			// add all elements from the overflow bucket
			while ( index < overflowBucket.getCurrentSize()){
				Object pluck = overflowBucket.data[index][0];
				currentContents.add(pluck);
				System.out.println("Overflow item " + pluck);
				index++;
			}
		
			// reset this overflow bucket
			splitBucket.resetBucket(this.overFlowPath, splitBucket.getOverflowOffset() - overFlowBucket1 * sizeOfBucket(), this.dataType);
			splitBucket.freeBuckets.add(overflowBucket);
			overFlowBucket1++;
		}
		// all contents of bucket to be split and its overflow buckets now in currentContents
		splitBucket.resetBucket(this.path, this.headerLength + (long) this.nextPointer * sizeOfBucket(), this.dataType);
		index = 0;
		System.out.println("Rehashing bucket");
		this.nextPointer++;
		System.out.println(this.nextPointer);
		while(index < currentContents.size()){
			System.out.println("Overflow!" + index);
			Object data = currentContents.get(index);
			int hash = getHash(data);
			System.out.println("New hash " + hash);
			this.writeToIndexFile(data, 0);
			index++;	
		}
		
		
		
		if(this.nextPointer == this.numberOfBuckets * (this.round + 1) - 1){
			this.nextPointer = 0;
			this.numberOfBuckets *= 2;
			this.round++;
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

	public ArrayList<Long> getListOfRIDsForColumnValue(Object value) {
		// Calculate the bucket where we need to look 
		// for RIDs
		Integer bucketToBeSearched = getHash(value);
		Long bucketToBeSearchedOffset = this.headerLength + (long) ((bucketToBeSearched)*sizeOfBucket());

		ArrayList<Long> retValues = new ArrayList<Long>();
		
		Bucket search = new Bucket(numberOfEntriesInBucket, (long)-1);
		search = search.readBucketFromFile(path, bucketToBeSearchedOffset, dataType);
		
		do{ // Read the Index bucket and all the overflow buckets.
			for (int i = 0 ; i <search.getCurrentSize() ; i++){
				// for each bucket - read all the data values.
				if (search.data[i][0] == value)
					// If value in the data array matches 
					// the value that we are searching 
					// add the RID to the list.
					retValues.add((Long) search.data[i][1]);
			}
			// Read the next Overflow bucket into memory
			search = search.readBucketFromFile(overFlowPath, search.getOverflowOffset(), dataType);
		}while (search != null);
		
		
		return retValues;
	}
	

	}
	
	



