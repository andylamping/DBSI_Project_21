package interfaces;
public interface Compare {

public int compare(byte[] record1,
             int offset1,
             byte[] record2,
             int offset2,
             int length);

public byte[] read(String path, int offset, int length);

public int write (String path, byte data, int offset, int length);

public long write (String path, long offset, String data, int length);

public long writeAtOffset(String path, long offset, String data, int length);

public String readString (String path, int offset, int length);
}