package simpledb;

public interface StreamReader {
	
	public Tuple getNext(int ts);
}
