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
import simpledb.StreamToRelationTupleWindowConverter;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;

public class StreamToRelationTupleWindowConverterTest {

	public class Pair<X, Y> { 
		public final X ele1; 
		public final Y ele2; 
		public Pair(X ele1, Y ele2) { 
		    this.ele1 = ele1; 
		    this.ele2 = ele2; 
		} 
	} 
	
	@Test
	public void SimpleStreamTestSmallWindow()
			throws NoSuchElementException, DbException, TransactionAbortedException {
		SimpleStreamTestHelper(4);
	}
	
	@Test
	public void SimpleStreamTestLargeWindow()
			throws NoSuchElementException, DbException, TransactionAbortedException {
		SimpleStreamTestHelper(40);
	}

	public void SimpleStreamTestHelper(int windowSize)
			throws NoSuchElementException, DbException, TransactionAbortedException {
		SimpleStreamReader sr = new SimpleStreamReader();
		Stream stream = new Stream(sr);
		
		HashMap<Integer, Integer> endTimes = new HashMap<Integer, Integer> ();
		
		// Insert tuple for timestamps upto 10 units
		for (int i = 0; i < 10; i++) {
			Tuple tuple = sr.addTuple();
    		IntField field;
    		int end = -1;
    		while (tuple != null) {
    			field = (IntField) tuple.getField(0);

    			end = field.getValue();
    			tuple = sr.addTuple();
    		}
    		endTimes.put(i, end);
		}
		
		StreamToRelationTupleWindowConverter converter = new StreamToRelationTupleWindowConverter(stream, windowSize, sr.getTupleDesc());
		boolean reachedCapacity = false;
		for (int i = 0; i < 10; i++) {
			converter.updateRelation();
			DbIterator iterator = converter.getRelation();
			iterator.open();

			int endTuple = endTimes.get(i);

			int n = 0;
			while (iterator.hasNext()) {
				Tuple tuple = iterator.next();
				if (endTuple != -1) {
					assertTrue(((IntField) tuple.getField(0)).getValue() >= endTuple - windowSize);
					assertTrue(((IntField) tuple.getField(0)).getValue() <= endTuple);
				}
				n++;
			}
			
			if (n == windowSize) {
				reachedCapacity = true;
			}

			// Assert number of tuples in this window is always less than or equal to the windowSize
			if (!reachedCapacity) {
				assertTrue(n < windowSize);
			} else {
				assertTrue(n == windowSize);
			}
			iterator.close();
		}
	}

}
