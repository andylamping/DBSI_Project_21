package util;

import java.util.ArrayList;

import util.CSVFile;

public class Output {
	private Query query;
	private int[] offsetList;
	public Output(Query inputQuery){
		this.query = inputQuery;
		processOutput();
	}
	
	private void processOutput() {
		if(this.query.matchingRecords != null){
			removeDuplicates();
			outputFile();
		}
		else if(this.query.hashRecords != null){
			outputFile();
		}
		
	}

	// removes duplicate records from the list of matching records
	public void removeDuplicates(){
	int m =  0;
	offsetList = this.query.heapFile.getOffsetList();
	ArrayList<Integer> conditionedRecords = new ArrayList<Integer>();

	int e = 0;
	int matchesNeeded = this.query.dummyRecord.size() - 1;

	while(e < this.query.matchingRecords.size()){
		int f = e + 1;
		int matched = 0;
		int matches = 0;
		while(matched == 0 && f < this.query.matchingRecords.size()){
			if(this.query.matchingRecords.get(e).equals(this.query.matchingRecords.get(f))){
				matches++;
			}
			if(matches == matchesNeeded){
				conditionedRecords.add(this.query.matchingRecords.get(e));
				matched = 1;
			}

			f++;
		}
		e++;
	}

	if(this.query.dummyRecord.size() > 1){
		this.query.matchingRecords = conditionedRecords;
	}
	
	}
	
	
	// prepare output file
	public void outputFileHash(){

	if(this.query.projectionList.isEmpty()){
		//						this.query.heapFile output = new this.query.heapFile("output", false, this.query.heapFile.schema, this.query.heapFile.schemaArray, null);
//		String outputString = this.query.heapFile.getCertainRecordsFromHeapFile(this.query.matchingRecords);
//		System.out.println(outputString);
//		ArrayList<String> s = new ArrayList<String>();
//		s.add(this.query.heapFile.schema+"\n");
//		s.add(outputString);
//		CSVFile output = new CSVFile("example_output.acsv", s);
//		output.getSchemaFromContents();
//		output.writeContentsToFile
//		output.writeRecordToCSVFileUsingBufferedWriter(query.heapFile);
		
		CSVFile output = new CSVFile("example_output.acsv");
		output.writeDataToFile(this.query.heapFile.schema+"\n");
		for (Long i:this.query.hashRecords){
			output.writeDataToFile(this.query.heapFile.getRecordByRIDFromHeapFile(i));
		}
		
	}
	else{
		String[] transfer = new String[this.query.projectionList.size()];
		// example p1, p3

		for(int a = 0; a < this.query.projectionList.size(); a++){
			transfer[a] = this.query.projectionList.get(a);
		}

		// still p1, p3 but in array
		int[] columns = new int[this.query.projectionList.size()];
		for (int b=0; b < this.query.projectionList.size(); b++){
			columns[b] =  Integer.parseInt(transfer[b].substring(transfer[b].length()-1));
		}
		// now [1,3]

		for(int c = 0; c < this.query.projectionList.size(); c++){
			columns[c] = columns[c] - 1;
		}
		// now [0,2] to correspond to schema

		// now create new schema and offsetList
		int d = 0;
		int[] projOffsetList = new int[columns.length];
		String subSchemaOfHeap[] = this.query.heapFile.schema.split(",");
		String projSubSchema[] = new String[columns.length];
		int[] projLengthArray = new int[columns.length];
		int[] projSchemaArray = new int[columns.length];

		while(d < columns.length){
			projLengthArray[d] = this.query.heapFile.lengthArray[d];
			projOffsetList[d] = offsetList[columns[d]];
			projSubSchema[d] = subSchemaOfHeap[columns[d]];
			projSchemaArray[d] = this.query.heapFile.schemaArray[columns[d]];
			d++;
		}

		//String output = this.query.heapFile.getProjectionRecords(this.query.matchingRecords, projSubSchema, projSchemaArray, projOffsetList, projLengthArray);
		ArrayList<String> projectionOutput = this.query.heapFile.getProjectionRecordsAsArrayList(this.query.matchingRecords, projSubSchema, projSchemaArray, projOffsetList, projLengthArray);
		//	CSVFile csvTarget = new CSVFile("example_result.acsv", output, 0);
		CSVFile csvTarget = new CSVFile("example_output", projectionOutput);
		csvTarget.getSchemaFromContents();
		csvTarget.writeRecordToCSVFileUsingBufferedWriter(query.heapFile);
	}
}
	
	public void outputFile(){

		if(this.query.projectionList.isEmpty()){
			//						this.query.heapFile output = new this.query.heapFile("output", false, this.query.heapFile.schema, this.query.heapFile.schemaArray, null);
//			String outputString = this.query.heapFile.getCertainRecordsFromHeapFile(this.query.matchingRecords);
//			System.out.println(outputString);
//			ArrayList<String> s = new ArrayList<String>();
//			s.add(this.query.heapFile.schema+"\n");
//			s.add(outputString);
//			CSVFile output = new CSVFile("example_output.acsv", s);
//			output.getSchemaFromContents();
//			output.writeContentsToFile
//			output.writeRecordToCSVFileUsingBufferedWriter(query.heapFile);
			
			CSVFile output = new CSVFile("example_output.acsv");
			output.writeDataToFile(this.query.heapFile.schema+"\n");
			for (Integer i:this.query.matchingRecords){
				output.writeDataToFile(this.query.heapFile.getRecordByRIDFromHeapFile(i));
			}
			
		}
		else{
			String[] transfer = new String[this.query.projectionList.size()];
			// example p1, p3

			for(int a = 0; a < this.query.projectionList.size(); a++){
				transfer[a] = this.query.projectionList.get(a);
			}

			// still p1, p3 but in array
			int[] columns = new int[this.query.projectionList.size()];
			for (int b=0; b < this.query.projectionList.size(); b++){
				columns[b] =  Integer.parseInt(transfer[b].substring(transfer[b].length()-1));
			}
			// now [1,3]

			for(int c = 0; c < this.query.projectionList.size(); c++){
				columns[c] = columns[c] - 1;
			}
			// now [0,2] to correspond to schema

			// now create new schema and offsetList
			int d = 0;
			int[] projOffsetList = new int[columns.length];
			String subSchemaOfHeap[] = this.query.heapFile.schema.split(",");
			String projSubSchema[] = new String[columns.length];
			int[] projLengthArray = new int[columns.length];
			int[] projSchemaArray = new int[columns.length];

			while(d < columns.length){
				projLengthArray[d] = this.query.heapFile.lengthArray[d];
				projOffsetList[d] = offsetList[columns[d]];
				projSubSchema[d] = subSchemaOfHeap[columns[d]];
				projSchemaArray[d] = this.query.heapFile.schemaArray[columns[d]];
				d++;
			}

			//String output = this.query.heapFile.getProjectionRecords(this.query.matchingRecords, projSubSchema, projSchemaArray, projOffsetList, projLengthArray);
			ArrayList<String> projectionOutput = this.query.heapFile.getProjectionRecordsAsArrayList(this.query.matchingRecords, projSubSchema, projSchemaArray, projOffsetList, projLengthArray);
			//	CSVFile csvTarget = new CSVFile("example_result.acsv", output, 0);
			CSVFile csvTarget = new CSVFile("example_output", projectionOutput);
			csvTarget.getSchemaFromContents();
			csvTarget.writeRecordToCSVFileUsingBufferedWriter(query.heapFile);
		}
	}
}


