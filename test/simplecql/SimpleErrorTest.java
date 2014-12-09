package simplecql;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

import simpledb.Aggregate;
import simpledb.Aggregator;
import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.FileStreamReader;
import simpledb.Filter;
import simpledb.IntField;
import simpledb.Operator;
import simpledb.Predicate;
import simpledb.RelationToIstreamConverter;
import simpledb.Stream;
import simpledb.StreamReader;
import simpledb.StreamToRelationTimeWindowConverter;
import simpledb.StringField;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.TupleIterator;
import simpledb.Type;
import simpledb.Predicate.Op;

public class SimpleErrorTest {
	
	@Test
	public void streamingErrorTestNaive() throws IOException, DbException, TransactionAbortedException {
		TupleDesc td = new TupleDesc(new Type[] { Type.STRING_TYPE });
		TupleDesc ntd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE });
		TupleDesc otd = new TupleDesc(new Type[] { Type.INT_TYPE });
		
		StreamReader isr = new FileStreamReader("simple_error_log.txt", td);
		Stream logStream = new Stream(isr);
		
		ArrayList<Tuple> allTuples = new ArrayList<Tuple> ();
		
		StreamReader expectedSr = new FileStreamReader("simple_error_log_output.txt", otd);
		Stream expectedStream = new Stream(expectedSr);
		for (int i = 0; i < 6; i++) {
			Tuple tuple = logStream.getNext(i);
			while (tuple != null) {
				Tuple newTuple = new Tuple(ntd);
				newTuple.setField(0, tuple.getField(0));
				newTuple.setField(1, new IntField(i));
				allTuples.add(newTuple);
				tuple = logStream.getNext(i);
			}
			
			Operator filter1 = new Filter(
					new Predicate(1, Op.GREATER_THAN_OR_EQ, new IntField(i)),
					new TupleIterator(ntd, allTuples));
			DbIterator intermediate1 = Utility.applyOperator(ntd, filter1);
			
			Operator filter2 = new Filter(
					new Predicate(0, Op.EQUALS, new StringField("server_500", Type.STRING_LEN)),
					intermediate1);
			DbIterator intermediate2 = Utility.applyOperator(ntd, filter2);
			
			Operator aggregate = new Aggregate(intermediate2, 0, -1, Aggregator.Op.COUNT);
			
			DbIterator output = Utility.applyOperator(otd, aggregate);
			
			Utility.checkEqualityBetweenStreamAndRelation(expectedStream, output, i);
		}
	}

	@Test
	public void streamingErrorTest() throws Exception {
		TupleDesc td = new TupleDesc(new Type[] { Type.STRING_TYPE });
		TupleDesc otd = new TupleDesc(new Type[] { Type.INT_TYPE });

		StreamReader isr = new FileStreamReader("simple_error_log.txt", td);

		Stream logStream = new Stream(isr);
		
		StreamToRelationTimeWindowConverter insertStreamConverter = new StreamToRelationTimeWindowConverter(logStream, 0, td);
		
		RelationToIstreamConverter rToSConverter = new RelationToIstreamConverter(otd);

		for (int i = 0; i < 6; i++) {
			DbIterator messages = insertStreamConverter.updateRelation();	
			
			Operator filter = new Filter(new Predicate(0, Op.EQUALS, new StringField("server_500", Type.STRING_LEN)), messages);
			DbIterator intermediate = Utility.applyOperator(td, filter);
			
			Operator aggregate = new Aggregate(intermediate, 0, -1, Aggregator.Op.COUNT);
			
			DbIterator output = Utility.applyOperator(otd, aggregate);
			
			rToSConverter.updateStream(output);
			
		}
		
		StreamReader expectedSr = new FileStreamReader("simple_error_log_output.txt", otd);
		Stream expectedStream = new Stream(expectedSr);
		Stream outputStream = rToSConverter.getStream();

		Utility.checkEquality(expectedStream, outputStream, 6);
	}
}
