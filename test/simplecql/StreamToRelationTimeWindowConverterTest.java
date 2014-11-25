package simplecql;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.NoSuchElementException;

import org.junit.Test;

import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.IntField;
import simpledb.SimpleStreamReader;
import simpledb.Stream;
import simpledb.StreamToRelationTimeWindowConverter;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;

public class StreamToRelationTimeWindowConverterTest {
	
	public class Pair<X, Y> { 
		public final X ele1; 
		public final Y ele2; 
		public Pair(X ele1, Y ele2) { 
		    this.ele1 = ele1; 
		    this.ele2 = ele2; 
		} 
	} 
	
	@Test
	public void SimpleStreamTestZeroWindow()
			throws NoSuchElementException, DbException, TransactionAbortedException {
		SimpleStreamTestHelper(0);
	}
	
	@Test
	public void SimpleStreamTestSmallWindow()
			throws NoSuchElementException, DbException, TransactionAbortedException {
		SimpleStreamTestHelper(2);
	}
	
	@Test
	public void SimpleStreamTestLargeWindow()
			throws NoSuchElementException, DbException, TransactionAbortedException {
		SimpleStreamTestHelper(10);
	}

	public void SimpleStreamTestHelper(int windowSize) throws NoSuchElementException, DbException, TransactionAbortedException {
		SimpleStreamReader sr = new SimpleStreamReader();
		Stream stream = new Stream(sr);
		
		HashMap<Integer, Pair<Integer, Integer>> startAndEndTimes = new HashMap<Integer, Pair<Integer, Integer>> ();
		
		// Insert tuple for timestamps upto 10 units
		for (int i = 0; i < 10; i++) {
			Tuple tuple = sr.addTuple();
    		IntField field;
    		int start = -1;
    		int end = -1;
    		boolean isFirst = true;
    		while (tuple != null) {
    			field = (IntField) tuple.getField(0);
    			if (isFirst) {
    				start = field.getValue();
    				isFirst = false;
    			}
    			end = field.getValue();
    			tuple = sr.addTuple();
    		}
    		startAndEndTimes.put(i, new Pair<Integer, Integer>(start, end));
		}
		
		StreamToRelationTimeWindowConverter converter = new StreamToRelationTimeWindowConverter(stream, windowSize, sr.getTupleDesc());
		for (int i = 0; i < 10; i++) {
			converter.updateRelation();
			DbIterator iterator = converter.getRelation();
			iterator.open();

			int startTime = (i - windowSize >= 0) ? (i - windowSize) : 0;
			int startTuple = startAndEndTimes.get(startTime).ele1;
			int endTuple = startAndEndTimes.get(i).ele2;

			while (iterator.hasNext()) {
				Tuple tuple = iterator.next();
				if (startTuple != -1 && endTuple != -1) {
					assertTrue(((IntField) tuple.getField(0)).getValue() >= startTuple);
					assertTrue(((IntField) tuple.getField(0)).getValue() <= endTuple);
				}
			}
			iterator.close();
		}
	}

}
