package Tester;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import compare.Comparer;

import util.CSVFile;
import util.Condition;
import util.HeapFile;
import util.Output;
import util.Record;
import util.Query;



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
			//	else if(args[2].equals("<")){

				// check for valid .csv file extension
	//			if(!args[3].contains(".acsv")){
		//			System.out.println("Please enter valid .acsv file.");
				//	return;
		//		}

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
					CSVFile csvSource = new CSVFile(args[argIndex]);
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
					CSVFile csvSource = new CSVFile(args[argIndex], null);
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
				// we want to either query the heapfile or build an index
				
				if(args[1].contains("-b")){
					// build index
					return;
				}
				else{
				HeapFile heapFile = new HeapFile(args[0], true, null, null, null);

				Query query = new Query(heapFile, args);
				return;
				}
			
/**
				//ArrayList<Condition> conditionList = new ArrayList<Condition>();
				//ArrayList<Condition> multiList = new ArrayList<Condition>();
				ArrayList<String> projectionList = new ArrayList<String>();
				int argIndex = 1;
				int argCount = 0;
				int multiCondition = 0;
				//int multiCount = 0;
				ArrayList<ArrayList<Condition>> dummyRecords  = new ArrayList<ArrayList<Condition>>();
				ArrayList<Condition> first = new ArrayList<Condition>();
				dummyRecords.add(first);
				// traverse command line and add condition(s) to conditionList
				while(argIndex <= (args.length - 1)){
					// if argument contains an s, then we create a new condition, 
					//and advance 3 spots in the index
					if(args[argIndex].contains("s")){
						argCount++;
						int columnNumber = Integer.parseInt(args[argIndex].substring(2));
						if(columnNumber > heapFile.numberOfFields  || columnNumber == 0){
							System.out.println("Sorry. That column for query does not exist.");
							return;
						}
						if(argCount > 1 && args[argIndex].equals(args[argIndex - 3])){
							Condition condition = new Condition(args[argIndex], args[argIndex + 1], args[argIndex + 2]);
							//	multiList.add(condition);
							multiCondition++;
							if(multiCondition >= dummyRecords.size()){
								ArrayList<Condition> next = new ArrayList<Condition>();
								dummyRecords.add(next);
								dummyRecords.get(multiCondition).add(condition);
							}
							else{
								dummyRecords.get(multiCondition).add(condition);
							}
							argIndex = argIndex + 3;

						}
						else{
							multiCondition = 0;
							Condition condition = new Condition(args[argIndex], args[argIndex + 1], args[argIndex + 2]);
							dummyRecords.get(multiCondition).add(condition);
							argIndex = argIndex + 3;

						}
					}

					// if argument contains a p, as in -p1, add this arg to projections and advance to next index
					if(argIndex <= (args.length - 1) && args[argIndex].contains("p")){
						int columnNumber = Integer.parseInt(args[argIndex].substring(2));
						if(columnNumber > heapFile.numberOfFields  || columnNumber == 0){
							System.out.println("Sorry. That column for projection does not exist.");
							return;
						}
						projectionList.add(args[argIndex]);
						argIndex++;
					}
				} // end of conditionlist, multilist maker and projectionlist maker

				// prepare heap file
**/
				/**
				Comparer comparer = new Comparer();
				ArrayList<Integer> matchingRecords = new ArrayList<Integer>();

				int m =  0;
				int[] offsetList = heapFile.getOffsetList();

				//				int firstListCheck = 0;
				while(m < query.dummyRecord.size()){

					int[] compareList = new int[heapFile.schemaArray.length];
					Record dummyRec = new Record();

					compareList = dummyRec.writeDummyFile(query.dummyRecord.get(m), compareList, heapFile);


					// create RAF to read heapFile

					int[] lengthList = heapFile.getListOfLengths();
					RandomAccessFile dummy;
					try {
						dummy = new RandomAccessFile(new File("dummy"), "rw");
						RandomAccessFile raf1 = new RandomAccessFile(new File(heapFile.path), "r");

						int currentRecord = 0;


						while(currentRecord < heapFile.numberOfRecords){

							raf1.seek(heapFile.currentFileOffset + (heapFile.numberOfBytesPerRecord * currentRecord));
							byte[] heapRec = new byte[heapFile.numberOfBytesPerRecord];
							raf1.read(heapRec);

							dummy.seek(0);
							byte[] dumRec = new byte[heapFile.numberOfBytesPerRecord];
							dummy.read(dumRec);

							Record results = new Record();
							int index = 0;
							int reject = 0;
							int match;
							int condIndex1 = 0;
							while(index < compareList.length && reject == 0){
								int answer;
								if(compareList[index] == 1){
									answer = comparer.compare_functions[heapFile.schemaArray[index]].compare(dumRec, offsetList[index], heapRec, offsetList[index],lengthList[index]);
									match = results.checkCompareResult(query.dummyRecord.get(m).get(condIndex1).operator, answer);
									if(match == 0){
										reject = 1;
									}
									condIndex1++;
								}

								index++;
							}

							if(reject == 0){
								// match found, add to matchRecords list
								matchingRecords.add(currentRecord);
							}

							currentRecord++;
						} // end of scanning all records

						dummy.close();
						File dummy1 = new File("dummy");
						dummy1.delete();
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} // end of catchers
					m++;
				} // end of m > 0 loop
**/
				/**
				ArrayList<Integer> conditionedRecords = new ArrayList<Integer>();

				int e = 0;
				int matchesNeeded = query.dummyRecord.size() - 1;

				while(e < query.matchingRecords.size()){
					int f = e + 1;
					int matched = 0;
					int matches = 0;
					while(matched == 0 && f < query.matchingRecords.size()){
						if(query.matchingRecords.get(e).equals(query.matchingRecords.get(f))){
							matches++;
						}
						if(matches == matchesNeeded){
							conditionedRecords.add(query.matchingRecords.get(e));
							matched = 1;
						}

						f++;
					}
					e++;
				}

				if(query.dummyRecord.size() > 1){
					query.matchingRecords = conditionedRecords;
				}
**/
				
			
				/**
				if(query.projectionList.isEmpty()){
					//	HeapFile output = new HeapFile("output", false, heapFile.schema, heapFile.schemaArray, null);
					String outputString = heapFile.getCertainRecordsFromHeapFile(query.matchingRecords);
					System.out.println(outputString);
					ArrayList<String> s = new ArrayList<String>();
					s.add(heapFile.schema+"\n");
					s.add(outputString);
					CSVFile outputCSV = new CSVFile("example_output.acsv", s);
					outputCSV.getSchemaFromContents();
					outputCSV.writeRecordToFileUsingBufferedWriter();
				}
				else{
					String[] transfer = new String[query.projectionList.size()];
					// example p1, p3

					for(int a = 0; a < query.projectionList.size(); a++){
						transfer[a] = query.projectionList.get(a);
					}

					// still p1, p3 but in array
					int[] columns = new int[query.projectionList.size()];
					for (int b=0; b < query.projectionList.size(); b++){
						columns[b] =  Integer.parseInt(transfer[b].substring(transfer[b].length()-1));
					}
					// now [1,3]

					for(int c = 0; c < query.projectionList.size(); c++){
						columns[c] = columns[c] - 1;
					}
					// now [0,2] to correspond to schema

					// now create new schema and offsetList
					int d = 0;
					int[] projOffsetList = new int[columns.length];
					String subSchemaOfHeap[] = heapFile.schema.split(",");
					String projSubSchema[] = new String[columns.length];
					int[] projLengthArray = new int[columns.length];
					int[] projSchemaArray = new int[columns.length];

					while(d < columns.length){
						projLengthArray[d] = heapFile.lengthArray[d];
						projOffsetList[d] = offsetList[columns[d]];
						projSubSchema[d] = subSchemaOfHeap[columns[d]];
						projSchemaArray[d] = heapFile.schemaArray[columns[d]];
						d++;
					}

					//String output = heapFile.getProjectionRecords(matchingRecords, projSubSchema, projSchemaArray, projOffsetList, projLengthArray);
					ArrayList<String> projectionOutput = heapFile.getProjectionRecordsAsArrayList(matchingRecords, projSubSchema, projSchemaArray, projOffsetList, projLengthArray);
					//	CSVFile csvTarget = new CSVFile("example_result.acsv", output, 0);
					CSVFile csvTarget = new CSVFile("example_output", projectionOutput);
					csvTarget.getSchemaFromContents();
					csvTarget.writeRecordToFileUsingBufferedWriter();
				}
// 2be87e762873cd632141416b87b4b55ca95a9709

**/
			} // end of query brackets



		} // end of args.length > 1
		
		}
	} // end of main


