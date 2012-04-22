package Tester;

import java.io.File;

import util.Bucket;
import util.IndexFile;

public class Flusher {

	public static void main (String args[]){

//		String s = "Day-Lewis12345678901234567890";
//
//		System.out.println(Helper.toString(Helper.toByta(s)));
//		
//		Float a = new Float(3.2);
//		Object o = a;
//		System.out.println("Hashcode " + o.hashCode());
//		
		
//		Bucket b = new Bucket(4, (long) 200);
//		b.writeBucketToFile("flush", (long) 0, "i4");
//		
//		Bucket c = b.readBucketFromFile("flush", (long) 0, "i4");
//		
//		System.out.println("Print B");
//		System.out.println(b);
//		System.out.println("Print C");
//		System.out.println(c);
//		System.out.println("done!");
		
		IndexFile indexFileTest = new IndexFile("indexFILE", "overflowFile","i4");
		indexFileTest.writeHeaderInformationToFile();
		indexFileTest.writeInitialBucketsToFile();
		long ptr = 0;
		for (int i = 0; i<10; i++){
			indexFileTest.writeToIndexFile(i, ptr);
			ptr +=10;
		}
		File f1 = new File("indexFILE");
		f1.delete();
		File f2 = new File("overflowFile");
		f2.delete();
	}


}
