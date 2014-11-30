package simplecql;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.NoSuchElementException;

import org.junit.Test;

import simpledb.Aggregate;
import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.FileStreamReader;
import simpledb.Filter;
import simpledb.IntField;
import simpledb.Operator;
import simpledb.Predicate;
import simpledb.Predicate.Op;
import simpledb.Aggregator;
import simpledb.SimpleStreamReader;
import simpledb.Stream;
import simpledb.StreamReader;
import simpledb.StreamToRelationTimeWindowConverter;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;

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

	private void SimpleStreamTestHelper(int windowSize)
			throws NoSuchElementException, DbException, TransactionAbortedException {
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
			DbIterator iterator = converter.updateRelation();
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
	
	@Test
	public void FileStreamWithAggregationTest() throws IOException, DbException, TransactionAbortedException {
		TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.TS_TYPE});
		TupleDesc aggregateDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
		StreamReader sr = new FileStreamReader("aggregation_test.txt", td);
		
		Stream stream = new Stream(sr);
		StreamToRelationTimeWindowConverter converter = new StreamToRelationTimeWindowConverter(stream, 0, td);
		DbIterator merge = null;
		// aggregation_test has data for 50 timesteps
		for (int i = 0; i < 50; i++) {
			DbIterator input = converter.updateRelation();
			Operator filter = new Filter(new Predicate(1, Op.GREATER_THAN_OR_EQ, new IntField(20)), input);
			DbIterator intermediate = Utility.applyOperator(td, filter);
			Operator aggregate = new Aggregate(intermediate, 1, -1, Aggregator.Op.AVG);
			DbIterator output = Utility.applyOperator(aggregateDesc, aggregate);
			merge = Utility.mergeIterators(aggregateDesc, merge, output);
		}
		
		StreamReader outputSr = new FileStreamReader("aggregation_test_output.txt",
				new TupleDesc(new Type[]{Type.INT_TYPE, Type.TS_TYPE}));
		Stream outputStream = new Stream(outputSr);

		Utility.checkEquality(merge, outputStream);
	}

}
