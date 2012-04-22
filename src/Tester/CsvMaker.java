package Tester;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class CsvMaker {

	public static void main (String args[]){
		
		String firstNames [] = {"Ashish", "Andrew", "Kanna",
				"Sandeep", "Dhaval","Alex","Orestis","Jaimin",
				"Harpreet","Vipul"};

		String lastNames[] = {"Chhabria", "Lamping", "Thirunarayanan",
				"Ranganathan", "Parekh", "Biliris", "Polychroniou","Shah",
				"Singh", "Singh"};
		
		String year[] = {"1902", "1901","1990","1989",
				"1992","1995","2012","1965","2000"};
		
		String average[] = {"1.87","1.76","2.54","2.77","4.5",
				"2.21","3.54","1.67","1.78"};
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter("input.acsv"));
			bw.write("c9,c9,i2,r4" +"\n");
			for (int i = 0; i < 10000; i++){
				bw.write(firstNames[(int) (Math.random()*10)]+","+
						lastNames[(int) (Math.random()*10)]+","+
						year[(int)(Math.random()*9)]+","+
						average[(int)(Math.random()*9)]+"\n");
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}


