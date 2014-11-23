package simpledb;

public class SimpleStreamReader implements StreamReader {
	
	private int count;
	private TupleDesc td;
	
	public SimpleStreamReader() {
		this.td = new TupleDesc(new Type[]{Type.INT_TYPE, Type.TS_TYPE});
		this.count = 0;
	}

	@Override
	public Tuple getNext(int ts) {
		Tuple tuple = new Tuple(this.td);
		tuple.setField(0, new IntField(count));
		tuple.setField(1, new TSField(ts));
		
		count += 1;
		
		return tuple;
	}

}
