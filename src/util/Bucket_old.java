package util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class Bucket_old implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2303330829937097709L;

	private Integer maxSize;
//	private Integer currentSize;
//	public byte[][] data;
	HashMap<Object, Object> data ;
	private Long overflowOffset;

	/* 
	 * Size of instance of Bucket object in bytes 
	 */
	private static int sizeOfBucketInBytes ;

	/**
	 * List of free buckets
	 * - check this list if new bucket is required,
	 * 		if yes, user the bucket.
	 * 		else, create new bucket.
	 */
	public static ArrayList<Bucket_old> freeBucketList = new ArrayList<Bucket_old>();


	/*
	 * Converts an instance of type 'Bucket' 
	 * to a byte array
	 */
	public static byte[] serialize(Bucket_old obj) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os;
		try {
			os = new ObjectOutputStream(out);
			os.writeObject(obj);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		setSizeOfBucketInBytes(out.toByteArray().length);
		return out.toByteArray();
	}
	/*
	 * Converts a byte array to an Instance of 
	 * type 'Bucket'
	 */
	public static Bucket_old deserialize(byte[] data) {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		ObjectInputStream is;
		try {
			is = new ObjectInputStream(in);
			return (Bucket_old) is.readObject();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;

	}

	// Parameterized Constructor.
	public Bucket_old(Integer maxSize, Long overflowOffset ){
		this.maxSize = maxSize;
		this.data = new HashMap<Object, Object>(maxSize);
		this.overflowOffset = overflowOffset;
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

	public Long getOverflowOffset() {
		return overflowOffset;
	}

	public void setOverflowOffset(Long overflowOffset) {
		this.overflowOffset = overflowOffset;
	}
	public static int getSizeOfBucketInBytes() {
		return sizeOfBucketInBytes;
	}
	public static void setSizeOfBucketInBytes(int sizeOfBucketInBytes) {
		Bucket_old.sizeOfBucketInBytes = sizeOfBucketInBytes;
	}

}
