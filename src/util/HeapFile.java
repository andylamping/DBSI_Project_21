package util;

import helper.Helper;

import java.io.File;
import java.io.FileNotFoundException;
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
	public int numberOfRecords;
	public int numberOfBytesPerRecord;

	public long currentFileOffset = 0;
	public int numberOfBytesInSchema;
	boolean headerRead = false;

	public HeapFile (String path,boolean fileExists, 
			String schema,int schemaArray[],ArrayList<String> contents){

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
			this.writeHeaderInformationToFile();

		}
	}

	public void writeHeaderInformationToFile(){
		/**
		 * Write the header information to 
		 * the file, 
		 * Number of Records, Number of Fields, No. of Bytes in Schema
		 * Schema
		 */

		File f = new File(this.path);
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(f, "rw");

			raf.write(Helper.toByta(this.numberOfRecords));
			this.currentFileOffset = raf.getFilePointer();

			raf.write(Helper.toByta(this.numberOfFields));
			this.currentFileOffset = raf.getFilePointer();

			raf.write(Helper.toByta(this.numberOfBytesInSchema));
			this.currentFileOffset = raf.getFilePointer();

			raf.write(Helper.toByta(this.schema));
			this.currentFileOffset = raf.getFilePointer();

			raf.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void getHeaderInformationFromFile(){
		/**
		 * TODO Read the schema as byte array,
		 * Convert to String and then map to 
		 * comparer class.
		 */
		File f = new File(this.path);
		RandomAccessFile raf;
		try {
			raf = new RandomAccessFile(f, "rw");
			byte b[] = new byte [4];

			raf.read(b, 0, 4);
			this.numberOfRecords = Helper.toInt(b);
			this.currentFileOffset = raf.getFilePointer();


			raf.read(b, 0, 4);
			this.numberOfFields = Helper.toInt(b);
			this.currentFileOffset = raf.getFilePointer();

			raf.read(b, 0, 4);
			this.numberOfBytesInSchema = Helper.toInt(b);
			this.currentFileOffset = raf.getFilePointer();

			byte [] tempSchema = new byte [numberOfBytesInSchema];

			raf.read(tempSchema,0,numberOfBytesInSchema);
			this.schema = new String(tempSchema);
			this.currentFileOffset = raf.getFilePointer();

			raf.close();

			headerRead = true;

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void writeHeaderToFile(int schemaArray[]){

		File f = new File(this.path);
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "rw");
			// First line is the schema ;
			raf.write(this.schema.getBytes(),(int) this.currentFileOffset, this.schema.getBytes().length);
			this.currentFileOffset = raf.getFilePointer();
			// Second line is the number of fields;
			// Third line is the number of records;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	public void writeContentAsBytesToHeapFile(int schemaArray[], ArrayList<String> contents){
		String s;
		for (int i = 0; i < contents.size(); i++){
			s = contents.get(i);
			writeRecordAsByteToHeapFile(s);
		}
		if (Config.DEBUG) System.out.println("All records written to file");

	}
	private void writeRecordAsByteToHeapFile(String record){
		Comparer comparer = new Comparer();
		String s[] = record.split(",");
		for (int j = 0; j<s.length ; j++){
			comparer.compare_functions[schemaArray[j]].write(path, currentFileOffset, s[j], lengthArray[j]);
			if (Config.DEBUG) System.out.println("Record "+ j  +" written to the file");
		}
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

	public ArrayList<String> getContentsFromHeapFile(){
		if (!this.headerRead)
			this.getHeaderInformationFromFile();
		ArrayList<String> contentStrings = new ArrayList<String>();
		contentStrings.add(this.schema +"\n");
		for (int i = 0 ;i< this.numberOfRecords; i++)
			contentStrings.add (getRecordFromHeapFile());

		return contentStrings;
	}

	public String getRecordFromHeapFile(){
		String result = "";
		byte [] val;
		Comparer comparer = new Comparer();
		for(int i = 0; i<this.schemaArray.length; i++){
			val = comparer.compare_functions[schemaArray[i]].read(this.path,(int) this.currentFileOffset, this.lengthArray[i]);
			result+= comparer.compare_functions[schemaArray[i]].readString(this.path,(int) this.currentFileOffset, this.lengthArray[i]) + ",";
			this.currentFileOffset +=val.length;
		}

		//		System.out.println("+1 Record read from the heap");

		return result.substring(0, result.length()-1) +"\n";
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

	public void updateNumberOfRecordsInHeapFile (ArrayList<String> Contents){
		/**
		 * Read the 'Number of Records' from the heap file
		 * Replace with the new number of records
		 * Write back to file.
		 */
		File f = new File(this.path);
		RandomAccessFile raf ;

		try {
			raf = new RandomAccessFile(f, "rw");

			raf.seek(0);
			byte b[] = new byte [4];
			raf.read(b);
			int data = Helper.toInt(b);

			data += Contents.size();

			raf.seek(0);
			raf.write(Helper.toByta(data));

			raf.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return total;
	}


}
