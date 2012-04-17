package util;

import java.util.ArrayList;

public class Bucket {

	private Integer maxSize;
	private Integer currentSize;
	public byte[][] data;
	private Long overflowOffset;


	/**
	 * List of free buckets
	 * - check this list if new bucket is required,
	 * 		if yes, user the bucket.
	 * 		else, create new bucket.
	 */
	public static ArrayList<Bucket> freeBucketList = new ArrayList<Bucket>();

	// Default Constructor
	public Bucket (){

	}

	//Parameterized Constructor
	public Bucket(Integer maxSize, Integer currentSize, Long overflowOffset){
		this.setCurrentSize(currentSize);
		this.setMaxSize(maxSize);
		this.setOverflowOffset(overflowOffset);
	}

	/*
	 * Getter and Setter methods.
	 */
	public Integer getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(Integer maxSize) {
		this.maxSize = maxSize;
	}

	public Integer getCurrentSize() {
		return currentSize;
	}

	public void setCurrentSize(Integer currentSize) {
		this.currentSize = currentSize;
	}

	public Long getOverflowOffset() {
		return overflowOffset;
	}

	public void setOverflowOffset(Long overflowOffset) {
		this.overflowOffset = overflowOffset;
	}

}
