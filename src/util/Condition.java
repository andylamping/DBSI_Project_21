package util;

public class Condition {

	String parameter;
	public String operator;
	String value;
	int column;
	/**
	 * Parameterized Constructor
	 * @param param
	 * @param op
	 * @param val
	 */
	public Condition (String param, String op, String val, int col){
		parameter = param; 
		operator = op;
		value = val;
		column = col;
	}
	
}
