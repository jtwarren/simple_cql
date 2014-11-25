package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

public class SimpleStreamReader implements StreamReader {
	
	private int count;
	private TupleDesc td;
	private Random random;
	private int largestTimeSeenSoFar;
	
	// Data structure that keeps track of all tuples generated
	private HashMap<Integer, ArrayList<Tuple>> tuples;
	
	// Fields required to respond to getNext queries
	private Iterator<Tuple> currentIterator;
	private int currentGetterTime;
	
	public SimpleStreamReader() {
		td = new TupleDesc(new Type[]{Type.INT_TYPE, Type.TS_TYPE});
		count = 0;
		largestTimeSeenSoFar = 0;
		currentGetterTime = -1;
		
		tuples = new HashMap<Integer, ArrayList<Tuple>> ();
		tuples.put(largestTimeSeenSoFar, new ArrayList<Tuple> ());
		
		random = new Random();
	}

	@Override
	public Tuple getNext(int ts) {
		if (ts >= largestTimeSeenSoFar) {
			throw new RuntimeException("ts is too far out");
		}

		if (currentGetterTime != ts) {
			currentIterator = tuples.get(ts).iterator();
			currentGetterTime = ts;
		}
		try {
			return currentIterator.next();
		} catch (NoSuchElementException e) {
			return null;
		}
	}

	public Tuple addTuple() {
		// Return a null tuple with probability 0.1; also advance largestTimeSeenSoFar
		if (random.nextFloat() < 0.1) {
			largestTimeSeenSoFar++;
			tuples.put(largestTimeSeenSoFar, new ArrayList<Tuple> ());
			return null;
		}
		
		// With probability 0.9, return a tuple with an incremented value, but
		// same timestamp as before
		Tuple tuple = new Tuple(td);
		tuple.setField(0, new IntField(count));
		tuple.setField(1, new TSField(largestTimeSeenSoFar));
		
		count++;
		
		tuples.get(largestTimeSeenSoFar).add(tuple);
		return tuple;
	}

	@Override
	public TupleDesc getTupleDesc() {
		return td;
	}

}
