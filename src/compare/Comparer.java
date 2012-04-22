package compare;
import interfaces.Compare;

import java.util.ArrayList;


public class Comparer {

	public Compare[] compare_functions;
	public ArrayList<String> mapper;


	public Comparer (){
		compare_functions = new Compare[7];
		mapper = new ArrayList<String>();


		compare_functions[0] = new Compare_i1();
		compare_functions[1] = new Compare_i2();
		compare_functions[2] = new Compare_i4();
		compare_functions[3] = new Compare_i8();
		compare_functions[4] = new Compare_r4();
		compare_functions[5] = new Compare_r8();
		compare_functions[6] = new Compare_cx();

		mapper.add("i1");mapper.add("i2");
		mapper.add("i4");mapper.add("i8");
		mapper.add("r4");mapper.add("r8");
		mapper.add("cx");
	}
}