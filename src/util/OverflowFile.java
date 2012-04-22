package util;

public class OverflowFile {
	
	private String path;
	
	
	public long currentFileOffset;

	
	public OverflowFile(String overFlowPath) {
		this.path = overFlowPath;
		this.currentFileOffset = 0;
	}
	
	public long writeNewBucketToFile(Bucket b){
		
		long startAddressOfBucket = this.currentFileOffset;
		
		return startAddressOfBucket;
	}

	/* Getter and Setters */
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

}
