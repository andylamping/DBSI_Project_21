package Tester;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import util.Condition;
import util.HeapFile;
import util.Record;
import compare.Comparer;

public class Query {
	
	public HeapFile heapFile;
	private String[] args;
	public ArrayList<ArrayList<Condition>> dummyRecord;
	private int argIndex;
	public ArrayList<String> projectionList = new ArrayList<String>();
	public ArrayList<Integer> matchingRecords;
	
	public Query(HeapFile inputHeap, String[] arguments) {
		this.heapFile = inputHeap;
		this.args = arguments;
		this.dummyRecord  = new ArrayList<ArrayList<Condition>>();
		this.addConditions();
		this.addProjections();
		this.argIndex = 1;
		this.projectionList = new ArrayList<String>();
		
	}

	

	private void addConditions(){

		int argCount = 0;
		int multiCondition = 0;
	
		ArrayList<Condition> first = new ArrayList<Condition>();
		this.dummyRecord.add(first);
		
		// traverse command line and add condition(s) to conditionList
		while(this.argIndex <= (args.length - 1)){
			// if argument contains an s, then we create a new condition, 
			//and advance 3 spots in the index
			if(this.args[this.argIndex].contains("s")){
				argCount++;
				int columnNumber = Integer.parseInt(this.args[this.argIndex].substring(2));
				if(columnNumber > heapFile.numberOfFields  || columnNumber == 0){
					System.out.println("Sorry. That column for query does not exist.");
					return;
				}
				if(argCount > 1 && this.args[this.argIndex].equals(this.args[this.argIndex - 3])){
					Condition condition = new Condition(this.args[this.argIndex], this.args[this.argIndex + 1], this.args[this.argIndex + 2]);
					//	multiList.add(condition);
					multiCondition++;
					if(multiCondition >= dummyRecord.size()){
						ArrayList<Condition> next = new ArrayList<Condition>();
						dummyRecord.add(next);
						dummyRecord.get(multiCondition).add(condition);
					}
					else{
						dummyRecord.get(multiCondition).add(condition);
					}
					this.argIndex = this.argIndex + 3;

				}
				else{
					multiCondition = 0;
					Condition condition = new Condition(this.args[this.argIndex], this.args[this.argIndex + 1], this.args[this.argIndex + 2]);
					dummyRecord.get(multiCondition).add(condition);
					this.argIndex = this.argIndex + 3;

				}
			}
		
	}
		
		
	}
	
	
	private void addProjections() {

		// if argument contains a p, as in -p1, add this arg to projections and advance to next index
		if(this.argIndex <= (this.args.length - 1) && this.args[this.argIndex].contains("p")){
			int columnNumber = Integer.parseInt(this.args[this.argIndex].substring(2));
			if(columnNumber > heapFile.numberOfFields  || columnNumber == 0){
				System.out.println("Sorry. That column for projection does not exist.");
				return;
			}
			this.projectionList.add(this.args[this.argIndex]);
			this.argIndex++;
		}
	}



	public ArrayList<Integer> findMatchingRecords() {
		Comparer comparer = new Comparer();
		this.matchingRecords = new ArrayList<Integer>();
		int m =  0;
		int[] offsetList = this.heapFile.getOffsetList();

		//int firstListCheck = 0;
		while(m < this.dummyRecord.size()){

			int[] compareList = new int[this.heapFile.schemaArray.length];
			Record dummyRec = new Record();

			compareList = dummyRec.writeDummyFile(this.dummyRecord.get(m), compareList, this.heapFile);


			// create RAF to read heapFile

			int[] lengthList = this.heapFile.getListOfLengths();
			RandomAccessFile dummy;
			try {
				dummy = new RandomAccessFile(new File("dummy"), "rw");
				RandomAccessFile raf1 = new RandomAccessFile(new File(this.heapFile.path), "r");

				int currentRecord = 0;


				while(currentRecord < this.heapFile.numberOfRecords){

					raf1.seek(this.heapFile.currentFileOffset + (this.heapFile.numberOfBytesPerRecord * currentRecord));
					byte[] heapRec = new byte[this.heapFile.numberOfBytesPerRecord];
					raf1.read(heapRec);

					dummy.seek(0);
					byte[] dumRec = new byte[this.heapFile.numberOfBytesPerRecord];
					dummy.read(dumRec);

					Record results = new Record();
					int index = 0;
					int reject = 0;
					int match;
					int condIndex1 = 0;
					while(index < compareList.length && reject == 0){
						int answer;
						if(compareList[index] == 1){
							answer = comparer.compare_functions[this.heapFile.schemaArray[index]].compare(dumRec, offsetList[index], heapRec, offsetList[index],lengthList[index]);
							match = results.checkCompareResult(this.dummyRecord.get(m).get(condIndex1).operator, answer);
							if(match == 0){
								reject = 1;
							}
							condIndex1++;
						}

						index++;
					}

					if(reject == 0){
						// match found, add to matchRecords list
						this.matchingRecords.add(currentRecord);
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
		
		return this.matchingRecords;
	}
		
}

