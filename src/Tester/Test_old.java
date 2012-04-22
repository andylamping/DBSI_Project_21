package Tester;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import util.CSVFile;
import util.HeapFile;
import util.Query;



public class Test_old {

	public static void main (String args[]){

		/* If there is just 1 argument, we print all records
		 * from the heap file in a CSV
		 */
		if (args.length == 1){
			HeapFile heapFile = new HeapFile(args[0], true, null, null);

			CSVFile csvTarget = new CSVFile("example_result.acsv");
			// Write the information(Schema and Records to the 
			// CSVFile in the byte format.
			csvTarget.writeRecordToCSVFileUsingBufferedWriter(heapFile);

		}

		/*
		 * If there is more than one argument, then
		 * we either append files OR query the heap
		 * file.
		 */
		if (args.length > 1){

			//checking if -i token is the second argument
			if (args[1].equals("-i")){
				// now we are inserting new records into the heapfile
				// we have two situations: building an index in addtion to adding records or just adding new records
				int argIndex = 3;
				int indexToBuild = Integer.MAX_VALUE;
				// check for < token
				if(!args[2].equals("<")){
					// we are building/rebuilding an index
					// advance index in args as we now have an additional argument -b#
					argIndex++;
					// parse the # in the -b#
					indexToBuild = Integer.parseInt(args[2].substring(2));

				}


				/**
				 * If format is correct, we need to check if heapfile 
				 * already exists.
				 */
				CSVFile csvSource = new CSVFile(args[argIndex]);
				BufferedReader br;
				try {
					/*
					 * Get necessary information from the CSV File.
					 */
					br = new BufferedReader(new FileReader(args[argIndex]));
					csvSource.schema = csvSource.getSchemaFromFile(br);
					csvSource.getSchemaArrayFromSchema();
					br.close();

				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				//Check if heap file already exists.
				File f = new File(args[0]);
				if (!f.exists()){
					/*
					 * If file doesn't exist, we create a new file with
					 * the schema from the CSV File.
					 */
					HeapFile hfTarget = new HeapFile(args[0], false, csvSource.schema, csvSource.schemaArray);
					hfTarget.writeHeaderInformationToFile();
					hfTarget.writeCsvContentsToHeapFile(csvSource);
				}else{
					/*
					 * If file exists, we compare the schema of HeapFile 
					 * with the Schema of the CSVFile input.
					 */
					HeapFile hfTarget = new HeapFile(args[0], true, null, null);
					if (!(hfTarget.schema.equalsIgnoreCase(csvSource.schema)))
						System.out.println("Error: Schema of the CSV and Heap files do not match");
					else{
						/*
						 * If schema matches, we make append the records to the 
						 * heapfile and increment the number of records in the
						 * Heapfile header.
						 */
						hfTarget.writeCsvContentsToHeapFile(csvSource);
					}

				}

			}
			else if (!args[1].equals("-i")){
				/*
				 *  we want to either 
				 *  QUERY THE HEAPFILE or 
				 *  BUILD AN INDEX ( or MULTIPLE INDEXES)
				 */

				if(args[1].contains("-b")){
					// build index
					return;
				}
				else{
					//Query the heap file
					HeapFile heapFile = new HeapFile(args[0], true, null, null);

					Query query = new Query(heapFile, args);
					query.processQuery();
					return;
				}

			} // end of query brackets



		} // end of args.length > 1

	}
} // end of main


