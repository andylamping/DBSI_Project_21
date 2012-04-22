package util;
import compare.Comparer;


public class MyFile {

	public String path;
	public String schema;
	public int [] schemaArray;
	public int [] lengthArray;
	
	public void getSchemaArrayFromSchema(){
		Comparer comparer = new Comparer();
		String subSchema[] = this.schema.split(",");
		lengthArray = new int[subSchema.length];
		
		for(int j = 0; j < subSchema.length; j++){
			lengthArray[j] = Integer.parseInt(subSchema[j].substring(1));
		}
		
		int tempArray[] = new int [subSchema.length];

		for (int i = 0; i < subSchema.length; i++){
			if (subSchema[i].contains("c"))
				tempArray[i] = 6;
			else tempArray[i] = comparer.mapper.indexOf(subSchema[i]);
		}

		this.schemaArray = tempArray;
	}
	
}
