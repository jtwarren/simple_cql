package simpledb;

import java.util.ArrayList;

public class StreamToRelationTimeWindowConverter {
	
	private Stream stream;
	private int curTimestamp;
	private int windowSize;
	private TupleDesc td;
	private ArrayList<Tuple> relation;

	public StreamToRelationTimeWindowConverter(Stream stream, int windowSize, TupleDesc td) {
		this.stream = stream;
		this.windowSize = windowSize;
		this.td = td;

		curTimestamp = 0;
		relation = new ArrayList<Tuple>();
	}

	// This function can only be called when we know the stream has seen all tuples upto curTimestamp, that
	// is, it has seen at least one tuple with timestamp curTimestamp + 1
	public DbIterator updateRelation() {
		Tuple nextTuple = stream.getNext(curTimestamp);
		while (nextTuple != null) {
			relation.add(nextTuple);
			nextTuple = stream.getNext(curTimestamp);
		}
		
		if (curTimestamp - windowSize - 1 >= 0) {
			Tuple nextTupleToDelete = stream.getNext(curTimestamp - windowSize - 1);
			while (nextTupleToDelete != null) {
				relation.remove(nextTupleToDelete);
				nextTupleToDelete = stream.getNext(curTimestamp - windowSize - 1);
			}
		}
		
		curTimestamp++;
		return new TupleIterator(td, relation);
	}

}
