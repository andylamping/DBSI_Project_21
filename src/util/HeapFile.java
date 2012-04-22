package util;

import helper.Helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import compare.Comparer;
import config.Config;


public class HeapFile extends MyFile{

	/**
	 * File Header format
	 */
	ArrayList<byte[]> contents;
	public int numberOfFields;
	public int numberOfRecords = 0;
	public int numberOfBytesPerRecord;
	public int numberOfBytesInIndexData;
	private int hasNewRecords = 0;

	public long currentFileOffset = 0;
	public int numberOfBytesInSchema;
	boolean headerRead = false;
	public int [] indexData;

	// Intermediate FileOffsets
	public long offsetNumberOfRecords = 0;
	public long offsetNumberOfFields = 0;
	public long offsetNumberOfBytesInSchema = 0;
	public long offsetSchema = 0;
	public long offsetNumberOfBytesInIndexData = 0;
	public long offsetIndexData = 0;
	public long offsetEndOfHeader = 0;

	public HeapFile (String path,boolean fileExists, 
			String schema,int schemaArray[]){

		this.path = path;
		if (fileExists){
			/**
			 * If file already exists, extract schema from the file
			 */
			this.getHeaderInformationFromFile();
			this.getSchemaArrayFromSchema();
			this.getNumberOfBytesPerRecord();


		}else{
			/**
			 * If file doesn't exist, create a new file with 
			 * the given schema
			 */
			this.schema = schema;
			this.numberOfFields = schema.split(",").length;
			if(contents != null)
				this.numberOfRecords = contents.size();
			this.numberOfBytesInSchema = this.schema.getBytes().length;

			this.getNumberOfBytesPerRecord();
			this.getSchemaArrayFromSchema();
			this.indexData = new int [this.schemaArray.length];
			int i = 0;
			while (i < indexData.length){
				this.indexData[i] = 0;
				i++;
			}
			this.writeHeaderInformationToFile();
			System.out.println("heap file " + this.path +  " created");

		}
	}


	public void writeHeaderInformationToFile(){
		/*
		 * Write the header information to 
		 * the file, 
		 * Number of Records, Number of Fields, No. of Bytes in Schema
		 * Schema, No. of Bytes in Index Data, Index Data.
		 */

		File f = new File(this.path);
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(f, "rw");

			this.offsetNumberOfRecords = 0;
			raf.write(Helper.toByta(this.numberOfRecords));

			this.offsetNumberOfFields = this.currentFileOffset = raf.getFilePointer();
			raf.write(Helper.toByta(this.numberOfFields));

			this.offsetNumberOfBytesInSchema = this.currentFileOffset = raf.getFilePointer();
			raf.write(Helper.toByta(this.numberOfBytesInSchema));

			this.offsetSchema = this.currentFileOffset = raf.getFilePointer();
			raf.write(Helper.toByta(this.schema));

			//Total number of Bytes in the indexData.
			this.offsetNumberOfBytesInIndexData = this.currentFileOffset = raf.getFilePointer();
			raf.write(Helper.toByta(indexData.length*4));

			//Write the indexData.
			this.offsetIndexData = this.currentFileOffset = raf.getFilePointer();
			for (int i = 0 ; i < indexData.length; i ++){
				raf.write(Helper.toByta(indexData[i]));
			}
			this.currentFileOffset = raf.getFilePointer();
			System.out.println(this.currentFileOffset);
			raf.close();

		} catch (FileNotFoundException e) {
			System.out.println("Write header information - File not found!");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void getHeaderInformationFromFile(){
		/*
		 * Reads Header information from the file and 
		 * sets the appropriate data fields.
		 * Also, updates the required offset values.
		 */
		File f = new File(this.path);
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(f, "rw");
			byte b[] = new byte [4];

			this.offsetNumberOfRecords = 0;
			raf.read(b, 0, 4);
			this.numberOfRecords = Helper.toInt(b);

			this.offsetNumberOfFields = this.currentFileOffset = raf.getFilePointer();
			raf.read(b, 0, 4);
			this.numberOfFields = Helper.toInt(b);

			this.offsetNumberOfBytesInSchema = this.currentFileOffset = raf.getFilePointer();
			raf.read(b, 0, 4);
			this.numberOfBytesInSchema = Helper.toInt(b);

			this.offsetSchema = this.currentFileOffset = raf.getFilePointer();
			byte [] tempSchema = new byte [numberOfBytesInSchema];
			raf.read(tempSchema,0,numberOfBytesInSchema);
			this.schema = new String(tempSchema);

			this.offsetNumberOfBytesInIndexData = this.currentFileOffset = raf.getFilePointer();
			raf.read(b,0,4);
			this.numberOfBytesInIndexData = Helper.toInt(b);

			this.offsetIndexData = this.currentFileOffset = raf.getFilePointer();
			//			byte[] tempIndexData = new byte [4];

			this.indexData = new int [this.numberOfBytesInIndexData/4];
			for (int i = 0 ; i< (this.numberOfBytesInIndexData/4) ; i++){

				raf.read(b, 0, 4);
				this.indexData[i] = Helper.toInt(b);
				System.out.println("Index " + i + "in indexData is " + Helper.toInt(b));
			}
			// Offset after Header information has been read.
			this.offsetEndOfHeader = this.currentFileOffset = raf.getFilePointer();
			System.out.println(this.offsetEndOfHeader + " EOF");
			raf.close();

			headerRead = true;

		} catch (FileNotFoundException e) {
			System.out.println("Reading Header information- File not found!");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void updateIndexData (int [] newIndexData){
		// Get the union of the existing Index Data 
		// and the updated Index

		int []tempIndexData = new int[this.indexData.length];
		for (int i = 0; i< this.indexData.length ; i++){
			if (this.indexData[i] == 1 || newIndexData[i] == 1)
				
				tempIndexData[i] = 1;
		}

		this.indexData = tempIndexData;

		//Write the updated Index data to the header
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(new File(this.path), "rw");
			raf.seek(this.offsetIndexData);
			System.out.println(this.offsetIndexData);
		//	System.out.println("rewrote indexdata in heap");
			raf.write(Helper.toByta(indexData));
			//System.out.print(raf.getFilePointer() + "FP");
			raf.close();
		} catch (FileNotFoundException e) {
			System.out.println("To update the Index Data - The file cannot be found");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


	}

	public void writeCsvContentsToHeapFile(CSVFile csvFile ){

		/*
		 * Pulls out each record from the CSV file and writes it to
		 * the Heap file in bytes.
		 */
		String currentRecord = null ;
		long start = this.currentFileOffset;
		int count  = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(csvFile.path)));
			currentRecord = csvFile.getRecordFromFile(br);
			while (currentRecord != null){
				if (count == 0){
					this.schema = currentRecord;
				}else{
					writeRecordAsByteToHeapFile(currentRecord);
				}
				count++;
				currentRecord = csvFile.getRecordFromFile(br);
			}
			// Update the number of records in the Heapfile header.
			updateNumberOfRecordsInHeapFile(count-1);
			br.close();
			appendIndexes(start, count);
		} catch (FileNotFoundException e) {
			System.out.println("Write CSV contents to heap file - FILE NOT FOUND!");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void appendIndexes(long start, int newRecords) {
		int i = 0;
		while(i < this.indexData.length){
			if(this.indexData[i] == 1){
				String dataType = this.schema.split(",")[i];
				this.currentFileOffset = start;
				IndexFile iFile = new IndexFile(path+ "." +i+1+".lht", path+"."+i+1+".lho", dataType);
				String currentRecord ;
				Object data;
				long currentRecordPointer = start;
				for (int j= 0 ;j < newRecords; j++){
					
					currentRecord = this.getRecordFromHeapFile();
					System.out.println("Current record is" + currentRecord);
					data = getAppropriateData(currentRecord, j+1,dataType);
					if (data == null){
						if (Config.DEBUG) System.out.println("Data reading problem");
					}
					System.out.println("Inserting" + data);
					iFile.writeToIndexFile(data, currentRecordPointer);
					currentRecordPointer += numberOfBytesPerRecord;

			}
System.out.println("STUCKKK");
			}
			i++;
		}
		
	}


	/**
	 * 
	 * @param 	record - that is to be inserted.
	 * @return 	The byte offset at which the record has been inserted.
	 */
	public long writeRecordAsByteToHeapFile(String record){
		Comparer comparer = new Comparer();
		String s[] = record.split(",");
		long startOffsetForRecord = this.currentFileOffset;
		for (int j = 0; j<s.length ; j++){
			comparer.compare_functions[schemaArray[j]].write(path, currentFileOffset, s[j], lengthArray[j]);
			this.currentFileOffset += lengthArray[j];
		}
		if (Config.DEBUG) System.out.println("Record written to the file");

		return startOffsetForRecord;
	}


	public void getNumberOfBytesPerRecord (){
		String subSchema[] = this.schema.split(",");
		int sum = 0;
		for (int i=0; i<subSchema.length; i++){
			sum += Integer.parseInt(subSchema[i].substring(subSchema[i].length()-1));
		}

		this.numberOfBytesPerRecord = sum;
	}


	public int[] getOffsetList(){
		int[] offsetList = new int[this.schemaArray.length];
		String subSchema[] = this.schema.split(",");

		for (int i=0; i<subSchema.length; i++){
			offsetList[i] = Integer.parseInt(subSchema[i].substring(subSchema[i].length()-1));
		}

		int j;
		int i;
		for(j = subSchema.length - 1; j >= 0; j--){
			offsetList[j] = 0;

			i = j - 1;
			while(i >= 0){
				offsetList[j] += offsetList[i];
				i--;
			}	

		}
		return offsetList;
	}


	public int[] getListOfLengths(){
		int[] lengthList = new int[this.schemaArray.length];
		String subSchema[] = this.schema.split(",");

		for (int i=0; i<subSchema.length; i++){
			lengthList[i] = Integer.parseInt(subSchema[i].substring(subSchema[i].length()-1));
		}
		return lengthList;
	}


	public String getRecordFromHeapFile(){
		String result = "";
		byte [] val;
	//	System.out.println(this.currentFileOffset);
		Comparer comparer = new Comparer();
		for(int i = 0; i<this.schemaArray.length; i++){
			val = comparer.compare_functions[schemaArray[i]].read(this.path,(int) this.currentFileOffset, this.lengthArray[i]);
			result+= comparer.compare_functions[schemaArray[i]].readString(this.path,(int) this.currentFileOffset, this.lengthArray[i]) + ",";
			this.currentFileOffset +=val.length;
		}

		//		System.out.println("+1 Record read from the heap");

		return result.substring(0, result.length()-1);
	}

	public String getRecordByRIDFromHeapFile(Integer RID){
		String result = "";

		byte[] val;
		Comparer comparer = new Comparer();

		long position = this.currentFileOffset + this.numberOfBytesPerRecord * RID;
		for(int i = 0; i<this.schemaArray.length; i++){
			val = comparer.compare_functions[schemaArray[i]].read(this.path,(int) position, this.lengthArray[i]);
			result += comparer.compare_functions[schemaArray[i]].readString(this.path,(int) position, this.lengthArray[i]) + ",";
			position += val.length;
		}

		result = result.substring(0, result.length()-1)+"\n";
		return result;
	}
	
	public String getRecordByRIDFromHeapFile(Long RID){
		String result = "";

		byte[] val;
		Comparer comparer = new Comparer();

		long position = this.currentFileOffset + this.numberOfBytesPerRecord * RID;
		for(int i = 0; i<this.schemaArray.length; i++){
			val = comparer.compare_functions[schemaArray[i]].read(this.path,(int) position, this.lengthArray[i]);
			result += comparer.compare_functions[schemaArray[i]].readString(this.path,(int) position, this.lengthArray[i]) + ",";
			position += val.length;
		}

		result = result.substring(0, result.length()-1)+"\n";
		return result;
	}

	public String getCertainRecordsFromHeapFile(ArrayList<Integer> matchingRecords){
		String result = "";

		String total = "";
		//		total += this.schema + '\n';
		byte [] val;
		Comparer comparer = new Comparer();
		int j = 0;

		while( j < matchingRecords.size()){
			long position = this.currentFileOffset + this.numberOfBytesPerRecord * matchingRecords.get(j);
			for(int i = 0; i<this.schemaArray.length; i++){
				val = comparer.compare_functions[schemaArray[i]].read(this.path,(int) position, this.lengthArray[i]);
				result += comparer.compare_functions[schemaArray[i]].readString(this.path,(int) position, this.lengthArray[i]) + ",";
				position += val.length;
			}
			result = result.substring(0, result.length()-1) +"\n";
			total += result;
			result = "";
			j++;
		}


		return total;
	}

	public void updateNumberOfRecordsInHeapFile (int count){
		/**
		 * Read the 'Number of Records' from the heap file
		 * Replace with the new number of records
		 * Write back to file.
		 */
		File f = new File(this.path);
		RandomAccessFile raf ;

		try {
			raf = new RandomAccessFile(f, "rw");

			raf.seek(this.offsetNumberOfRecords);
			byte b[] = new byte [4];
			raf.read(b);
			int data = Helper.toInt(b);

			data += count;

			raf.seek(this.offsetNumberOfRecords);
			raf.write(Helper.toByta(data));

			raf.close();
		} catch (FileNotFoundException e) {
			System.out.println("Update Number of Records - File not found!");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public ArrayList<String> getProjectionRecordsAsArrayList(ArrayList<Integer> records, String[] schema, int[] projSchemaArray, int[] offsetList, int[] projLengthArray){
		ArrayList<String> projectionSchemaAndRecords = new ArrayList<String>();
		String tempSchema ="";
		try{
			int i = 0;
			while (i< schema.length){
				tempSchema += schema[i];
				if (i+1 < schema.length) 
					tempSchema += ",";
				i++;
			}
			projectionSchemaAndRecords.add(tempSchema+"\n");

			Comparer comparer = new Comparer();
			int j = 0;

			while( j < records.size()){
				String result = "";
				long position = this.currentFileOffset + this.numberOfBytesPerRecord * records.get(j);
				for(int c = 0; c< offsetList.length; c++){
					result += comparer.compare_functions[projSchemaArray[c]].readString(this.path,(int) position + offsetList[c], projLengthArray[c]) + ",";
				}
				result = result.substring(0, result.length()-1) +"\n";
				projectionSchemaAndRecords.add(result);

				j++;
			}

			return projectionSchemaAndRecords;
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}

	public String getProjectionRecords(ArrayList<Integer> records, String[] schema, int[] projSchemaArray, int[] offsetList, int[] projLengthArray ){

		String total = "";
		try {
			RandomAccessFile raf = new RandomAccessFile(new File("example_output"), "rw");
			int i = 0;
			// add schema
			while(i < schema.length){
				total += schema[i];
				if(i + 1 < schema.length)
					total += ",";
				i++;
			}
			total += '\n';
			raf.writeChars(total + '\n');

			Comparer comparer = new Comparer();
			int j = 0;

			while( j < records.size()){
				String result = "";
				long position = this.currentFileOffset + this.numberOfBytesPerRecord * records.get(j);
				for(int c = 0; c< offsetList.length; c++){
					result += comparer.compare_functions[projSchemaArray[c]].readString(this.path,(int) position + offsetList[c], projLengthArray[c]) + ",";
				}
				result = result.substring(0, result.length()-1) +'\n';
				raf.writeChars(result + '\n');
				total += result;

				j++;
			}

			System.out.println(total);
			return total;
		} catch (FileNotFoundException e) {
			System.out.println("Get Projection records - File not found!");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return total;
	}

	/*
	 * Accepts the column number and builds the 
	 * index on the column number.
	 */
	public void buildIndexOnColumn(Integer columnNumber){
		
		// Get header information from the heap file.
		this.getHeaderInformationFromFile();
		// Translate the columnNumber to a datatype
		String dataType = this.schema.split(",")[columnNumber-1];

	//	if (indexExistsOnColumn(columnNumber) && this.hasNewRecords == 0){
	//		System.out.println("already exists!");
			// If Index already exists on the given column number.

	//	}else{
			// If Index doesn't exist on the given column number.
			setIndexOnColumn(columnNumber);
			updateIndexData(indexData);
			// Create a new index.

			IndexFile iFile = new IndexFile(path+ "." +columnNumber+".lht", path+"."+columnNumber+".lho", dataType);
			iFile.writeHeaderInformationToFile();
			iFile.writeInitialBucketsToFile();
			this.getHeaderInformationFromFile();
			String currentRecord ;
			Object data;
			long currentRecordPointer = this.currentFileOffset;
			for (int i= 0 ;i < this.numberOfRecords; i++){
				currentRecord = this.getRecordFromHeapFile();
				System.out.println("Current record is" + currentRecord);
				data = getAppropriateData(currentRecord, columnNumber,dataType);
				if (data == null){
					if (Config.DEBUG) System.out.println("Data reading problem");
				}
				System.out.println("Inserting" + data);
				iFile.writeToIndexFile(data, currentRecordPointer);
				currentRecordPointer += numberOfBytesPerRecord;
		//	}



		}

	}


	private Object getAppropriateData(String currentRecord, Integer columnNumber ,String dataType) {
		int length = Integer.parseInt(dataType.substring(1));
		char type = dataType.charAt(0);

		Object retValue = null;
		String dataToBeReturned = currentRecord.split(",")[columnNumber -1];
		System.out.println("datatobereturned is " + dataToBeReturned);
		switch (type) {
		case 'c':
			// If data is a string, we return it as it is.
			retValue = dataToBeReturned;
			break;

		case 'i':
			// If data is an Integer 
				switch (length) {
				case 1:
					retValue = Byte.parseByte(dataToBeReturned);
					break;
					
				case 2:
					retValue = Short.parseShort(dataToBeReturned);
					break;
	
				case 4:
					retValue = Integer.parseInt(dataToBeReturned);
					break;
	
				case 8:
					retValue = Long.parseLong(dataToBeReturned);
					break;
	
				default:
					break;
				}
			break;

		case 'r':
			switch (length) {
			case 4:
				retValue = Float.parseFloat(dataToBeReturned);
				break;
			
			case 8:
				retValue = Double.parseDouble(dataToBeReturned);
			default:
				break;
			}
			break;
		default:
			break;
		}
		
		return retValue;
	}


	private void setIndexOnColumn(Integer columnNumber) {
		this.indexData[columnNumber - 1] = 1;
		System.out.println("index on column number " + columnNumber + " set to 1");
	}


/**
	/*
	 * Check if index already exists on the given column number.
	 */

	public boolean indexExistsOnColumn(Integer columnNumber) {
		System.out.println("Checking to see if index exists.");
		if(this.indexData[columnNumber-1] == 1){
			System.out.println("It does");
		}
		else
			System.out.println("It doesnt");
		return (this.indexData[columnNumber-1] == 1);
	}
	
	
	
	/*
	 * Returns a list of RIDs that match up to the record
	 */
	public ArrayList<Long> getListOfRidsForSelectionCondition(Integer columnNumber,Object value){
		String dataType = this.schema.split(",")[columnNumber-1];
		IndexFile iFile = new IndexFile(path+ "." +columnNumber+".lht", path+"."+columnNumber+".lho", dataType);
		
		return iFile.getListOfRIDsForColumnValue(value);
	}

}
