package Tester;

import util.Bucket;

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
		
		Bucket b = new Bucket(4, (long) 200);
//		System.out.println(Bucket.serialize(b));
//		System.out.println(Bucket.deserialize(Bucket.serialize(b)));
//		System.out.println(b.getClass());
		
		b.writeBucketToFile("flush", (long) 0, "i4");
		
		Bucket c = b.readBucketFromFile("flush", (long) 0, "i4");
		
		System.out.println("Print B");
		System.out.println(b);
		System.out.println("Print C");
		System.out.println(c);
		System.out.println("done!");
	}


}
