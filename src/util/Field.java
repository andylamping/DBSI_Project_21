package util;


public class Field {
	
	int fieldNo;
	int sizeInBytes;
	byte data;
	long offset;
	
	
	public Field(int fieldNo, int sizeInBytes, byte data, long offset) {
		super();
		this.fieldNo = fieldNo;
		this.sizeInBytes = sizeInBytes;
		this.data = data;
		this.offset = offset;
	}
	
	

}