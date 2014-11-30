package simplecql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;

import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.IntField;
import simpledb.Operator;
import simpledb.Stream;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.TupleIterator;

public class Utility {
	
	public static DbIterator applyOperator(
			TupleDesc td, Operator operator) throws DbException, TransactionAbortedException {
		ArrayList<Tuple> output = new ArrayList<Tuple> ();
		operator.open();
		while (operator.hasNext()) {
			output.add(operator.next());
		}
		operator.close();
		return new TupleIterator(td, output);
	}
	
	public static DbIterator mergeIterators(
			TupleDesc td, DbIterator iterator1, DbIterator iterator2) throws DbException, TransactionAbortedException {
		ArrayList<Tuple> merge = new ArrayList<Tuple> ();
		if (iterator1 != null) {
			iterator1.open();
			while (iterator1.hasNext()) {
				merge.add(iterator1.next());
			}
			iterator1.close();
		}
		iterator2.open();
		while (iterator2.hasNext()) {
			merge.add(iterator2.next());
		}
		iterator2.close();
		return new TupleIterator(td, merge);
	}
	
	public static void checkEquality(
			Stream expectedStream, Stream outputStream, int numTimeSteps) throws DbException, TransactionAbortedException {
		for (int ts = 0; ts < numTimeSteps; ts++) {
			Tuple expectedTuple = expectedStream.getNext(ts);
			while (expectedTuple != null) {
				Tuple outputTuple = outputStream.getNext(ts);
				assertNotNull(outputTuple);
				assertEquals(((IntField) expectedTuple.getField(0)).getValue(),
						((IntField) outputTuple.getField(0)).getValue());
				expectedTuple = expectedStream.getNext(ts);
			}
			assertNull(outputStream.getNext(ts));
		}
	}

}
