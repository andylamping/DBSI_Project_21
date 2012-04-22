package compare;
import helper.Helper;
import interfaces.Compare;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


public class Compare_cx implements Compare {

	@Override
	public int compare(byte[] record1, int offset1, byte[] record2,
			int offset2, int length) {
		byte [] val1 = new byte [length];
		byte [] val2 = new byte [length];
		int i = 0;
		while( i < length){
			val1[i] = record1[offset1 + i];
			val2[i] = record2[offset2 + i];
			i++;
		}
		String x = Helper.toString(val1);
		String y = Helper.toString(val2);



		// return x < y ? -1 : x > y ? 1 : 0;
		// return 0 if equal, 1 if different
		return x.equalsIgnoreCase(y) ? 0 : 1;
	}

	@Override
	public byte[] read(String path, int offset, int length) {
		// TODO Auto-generated method stub
		byte [] val = new byte [length];

		try {
			RandomAccessFile raf = new RandomAccessFile(new File(path), "rw");
			raf.seek(offset);
			raf.read(val,0,length);
			raf.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return val;
	}

	@Override
	public int write(String path, byte data, int offset, int length) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long write(String path, long offset, String data, int length) {
		// TODO Auto-generated method stub

		File f = new File(path);
		byte tempArray [] = Helper.toByta(data);
		byte b[] = new byte [length];
		if (b.length == tempArray.length) b = tempArray;
		else if (b.length < tempArray.length){
			for (int i = 0 ; i<b.length ; i++)
				b[i] = tempArray[i];
		}
		else if (b.length > tempArray.length){
			for (int i = 0 ; i<tempArray.length ; i++)
				b[i] = tempArray[i];
			for (int i=tempArray.length; i<b.length; i++)
				b[i] = 0x0;
		}
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "rw");
			raf.seek(f.length());
			raf.write(b);
			raf.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public String readString(String path, int offset, int length) {
		return Helper.toString(read(path, offset, length));
	}

	@Override
	public long writeAtOffset(RandomAccessFile raf, long offset, String data,
			int length) {
		byte tempArray [] = Helper.toByta(data);
		byte b[] = new byte [length];
		if (b.length == tempArray.length) b = tempArray;
		else if (b.length < tempArray.length){
			for (int i = 0 ; i<b.length ; i++)
				b[i] = tempArray[i];
		}
		else if (b.length > tempArray.length){
			for (int i = 0 ; i<tempArray.length ; i++)
				b[i] = tempArray[i];
			for (int i=tempArray.length; i<b.length; i++)
				b[i] = 0x0;
		}
		try {

			raf.seek(offset);
			raf.write(b);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public byte[] readAtOffset(RandomAccessFile raf, long offset, int length) {
		byte [] val = new byte [length];

		try {
			raf.seek(offset);
			raf.read(val,0,length);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return val;
	}

}