package Tester;
import java.io.File;
import java.util.ArrayList;

import util.CSVFile;
import util.Condition;
import util.HeapFile;



public class Test {

	public static void main (String args[]){

		/* If there is just 1 argument, we print all records
		 * from the heap file in a CSV
		 */
		if (args.length == 1){
			HeapFile heapFile = new HeapFile(args[0], true, null, null, null);
			ArrayList<String> contentsFromHeap = heapFile.getContentsFromHeapFile();
			if (contentsFromHeap== null){
				CSVFile csvTarget = new CSVFile("example_result.acsv");
				csvTarget.getContentsFromFile();
				csvTarget.getSchemaFromContents();
				csvTarget.getSchemaArrayFromSchema();
			}else{
				CSVFile csvTarget = new CSVFile("example_result.acsv",contentsFromHeap );
				csvTarget.contents = contentsFromHeap;
				csvTarget.getSchemaFromContents();
				csvTarget.writeRecordToFileUsingBufferedWriter();

			}
		}

		/*
		 * If there is more than one argument, then
		 * we either append files OR query the heap
		 * file.
		 */
		if (args.length > 1){

			//checking if -i token is the second argument
			if (args[1].equals("-i")){

				// check for < token
				if(!args[2].equals("<")){
					System.out.println("Incorrect format. Correct format: ./Tester heap_file_path -i < example.acsv");
					return;
				}

				// check for valid .csv file extension
				if(!args[3].contains(".acsv")){
					System.out.println("Please enter valid .acsv file.");
					return;
				}

				/**
				 * If format is correct, we need to check if heapfile already exists.
				 */
				//Check if heap file already exists.
				File f = new File(args[0]);
				if (!f.exists()){
					/*
					 * If file doesn't exist, we create a new file with the 
					 * schema from the CSV file.
					 */
					CSVFile csvSource = new CSVFile(args[3]);
					csvSource.getContentsFromFile();
					csvSource.getSchemaFromContents();
					csvSource.getSchemaArrayFromSchema();
					HeapFile hfNew = new HeapFile(args[0], false, csvSource.schema, csvSource.schemaArray, csvSource.contents);
					hfNew.writeContentAsBytesToHeapFile(csvSource.schemaArray, csvSource.contents);
				}else{
					/*
					 * If file exists, we compare the schema of the HeapFile 
					 * and the CSV file.
					 */
					CSVFile csvSource = new CSVFile(args[3], null);
					HeapFile hfNew = new HeapFile(args[0], true, null, null, csvSource.contents);
					if (!(hfNew.schema.equalsIgnoreCase(csvSource.schema))) 
						System.out.println("Error: The schema of the files do not match.");
					else {
						hfNew.writeContentAsBytesToHeapFile(hfNew.schemaArray, csvSource.contents);
						hfNew.updateNumberOfRecordsInHeapFile(csvSource.contents);

					}
				}

			}
			else if (!args[1].equals("-i")){
				// we want to query the file heapfile
				HeapFile heapFile = new HeapFile(args[0], true, null, null, null);
				Query query = new Query(heapFile, args);
				ArrayList<ArrayList<Condition>> dummyRecord  = query.dummyRecord;
				Output output = new Output(query);
			


			} // end of query brackets



		} // end of args.length > 1
	} // end of main
} // end of class


