package simpledb;

public class Stream {
	StreamReader sr;
	
	public Stream(StreamReader sr) {
		this.sr = sr;
	}
	
	public Tuple getNext(int ts) {
		return this.sr.getNext(ts);
	}
}
