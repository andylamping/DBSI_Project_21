package util;

public class Condition {

	String parameter;
	public String operator;
	String value;
	
	/**
	 * Parameterized Constructor
	 * @param param
	 * @param op
	 * @param val
	 */
	public Condition (String param, String op, String val){
		parameter = param; 
		operator = op;
		value = val;
	}
	
}
