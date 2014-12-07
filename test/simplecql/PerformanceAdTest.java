package simplecql;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.FileStreamReader;
import simpledb.Filter;
import simpledb.IntField;
import simpledb.Join;
import simpledb.JoinPredicate;
import simpledb.Operator;
import simpledb.Predicate;
import simpledb.RelationToIstreamConverter;
import simpledb.Stream;
import simpledb.StreamReader;
import simpledb.StreamToRelationTimeWindowConverter;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.TupleIterator;
import simpledb.Type;
import simpledb.Predicate.Op;

public class PerformanceAdTest {
	
	@Test
	public void timeWindowSystemTestNaive() throws IOException, DbException, TransactionAbortedException {
		long timestart = System.currentTimeMillis();
		TupleDesc itd = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE });
		TupleDesc nitd = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE });
		TupleDesc etd = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE });
		TupleDesc netd = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE });
		TupleDesc jtd = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE });

		StreamReader isr = new FileStreamReader("ad_insert.txt", itd);
		StreamReader est = new FileStreamReader("ad_event.txt", etd);

		Stream insertStream = new Stream(isr);
		Stream eventStream = new Stream(est);

		ArrayList<Tuple> allInsertTuples = new ArrayList<Tuple> ();
		ArrayList<Tuple> allEventTuples = new ArrayList<Tuple> ();
    	long timemid = System.currentTimeMillis();
		for (int i = 0; i < 1800; i++) {
			Tuple tuple;
			tuple = insertStream.getNext(i);
			while (tuple != null) {
				Tuple newTuple = new Tuple(nitd);
				for (int j = 0; j < 4; j++) {
					newTuple.setField(j, tuple.getField(j));
				}
				newTuple.setField(4, new IntField(i));
				allInsertTuples.add(newTuple);
				tuple = insertStream.getNext(i);
			}
			tuple = eventStream.getNext(i);
			while (tuple != null) {
				Tuple newTuple = new Tuple(netd);
				for (int j = 0; j < 3; j++) {
					newTuple.setField(j, tuple.getField(j));
				}
				newTuple.setField(3, new IntField(i));
				allEventTuples.add(newTuple);
				tuple = eventStream.getNext(i);
			}
			
			Operator filter1 = new Filter(
					new Predicate(4, Op.GREATER_THAN_OR_EQ, new IntField(i - 600)),
					new TupleIterator(nitd, allInsertTuples));
			DbIterator insertions = Utility.applyOperator(nitd, filter1);

			Operator filter2 = new Filter(
					new Predicate(3, Op.GREATER_THAN_OR_EQ, new IntField(i)),
					new TupleIterator(netd, allEventTuples));
			DbIterator events = Utility.applyOperator(netd, filter2);			
			
			JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 1);
	        Join joinOp = new Join(p, insertions, events);
	        
	        DbIterator output = Utility.applyOperator(jtd, joinOp);
			
		}
		
		long timeend = System.currentTimeMillis();
		
		System.out.println("Metrics for Naive implementation");
    	System.out.println(timemid / 1000 - timestart / 1000);
    	System.out.println(timeend / 1000 - timemid / 1000);
    	System.out.println("");
	}

	@Test
	public void timeWindowSystemTest() throws IOException, DbException, TransactionAbortedException {
		long timestart = System.currentTimeMillis();
		TupleDesc itd = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE });
		TupleDesc etd = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE });
		TupleDesc jtd = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE });

		StreamReader isr = new FileStreamReader("ad_insert.txt", itd);
		StreamReader est = new FileStreamReader("ad_event.txt", etd);

		Stream insertStream = new Stream(isr);
		Stream eventStream = new Stream(est);
		
		StreamToRelationTimeWindowConverter insertStreamConverter = new StreamToRelationTimeWindowConverter(insertStream, 600, itd);
		StreamToRelationTimeWindowConverter eventStreamConverter = new StreamToRelationTimeWindowConverter(eventStream, 0, etd);
		
		RelationToIstreamConverter rToSConverter = new RelationToIstreamConverter(jtd);

    	long timemid = System.currentTimeMillis();
		for (int i = 0; i < 1800; i++) {
			DbIterator insertions = insertStreamConverter.updateRelation();
			DbIterator events = eventStreamConverter.updateRelation();			
			
			JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 1);
	        Join joinOp = new Join(p, insertions, events);
	        
	        DbIterator output = Utility.applyOperator(jtd, joinOp);
			
			rToSConverter.updateStream(output);
			
		}
		
		long timeend = System.currentTimeMillis();
		
		System.out.println("Metrics for windowing implementation");
    	System.out.println(timemid / 1000 - timestart / 1000);
    	System.out.println(timeend / 1000 - timemid / 1000);
    	System.out.println("");

	}

}
