package Tester;

import helper.Helper;

public class Flusher {

	public static void main (String args[]){

		String s = "Day-Lewis12345678901234567890";

		System.out.println(Helper.toString(Helper.toByta(s)));
		
		Float a = new Float(3.2);
		Object o = a;
		System.out.println("Hashcode " + o.hashCode());
	}


}
