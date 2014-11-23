package simpledb;

import java.util.ArrayList;

public class StreamToRelationTupleWindowConverter {

	private Stream stream;
	private int curTimestamp;
	private int windowSize;
	private TupleDesc td;
	private ArrayList<Tuple> relation;

	public StreamToRelationTupleWindowConverter(Stream stream, int windowSize, TupleDesc td) {
		this.stream = stream;
		this.windowSize = windowSize;
		this.td = td;

		curTimestamp = 0;
		relation = new ArrayList<Tuple>();
	}

	public void updateRelation() {
		Tuple nextTuple = stream.getNext(curTimestamp);
		while (nextTuple != null) {
			relation.add(nextTuple);
			nextTuple = stream.getNext(curTimestamp);
		}
		
		while (relation.size() > windowSize) {
			relation.remove(0);
		}
		
		curTimestamp++;
	}
	
	public DbIterator getRelation() {
		return new TupleIterator(td, relation);
	}

}
