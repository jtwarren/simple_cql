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
import simpledb.Join;
import simpledb.JoinPredicate;
import simpledb.Operator;
import simpledb.Predicate;
import simpledb.Project;
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

public class PerformanceTrendingTest {

	@Test
	public void streamingTrendingNaiveTest() throws IOException, TransactionAbortedException, DbException {
		long timestart = System.currentTimeMillis();
		TupleDesc td = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.STRING_TYPE });
		TupleDesc ntd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.STRING_TYPE, Type.INT_TYPE });
		TupleDesc mtd = new TupleDesc(new Type[] { Type.INT_TYPE });
		TupleDesc otd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE });
		TupleDesc jtd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE, Type.INT_TYPE });
		TupleDesc rtd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE });

		StreamReader isr = new FileStreamReader("trending.txt", td);
		
		ArrayList<Tuple> tweetTuples = new ArrayList<Tuple> ();

		long timemid = System.currentTimeMillis();
		for (int i = 0; i < 300; i++) {
			Tuple tuple = isr.getNext(i);
			while (tuple != null) {
				Tuple newTuple = new Tuple(ntd);
				newTuple.setField(0, tuple.getField(0));
				newTuple.setField(1, tuple.getField(1));
				newTuple.setField(2, new IntField(i));
				tweetTuples.add(newTuple);
				tuple = isr.getNext(i);
			}
			
			Operator filter1 = new Filter(
					new Predicate(2, Op.GREATER_THAN_OR_EQ, new IntField(i - 120)),
					new TupleIterator(ntd, tweetTuples));
			DbIterator tweets = Utility.applyOperator(ntd, filter1);

			Operator filter2 = new Filter(new Predicate(1, Op.EQUALS, new StringField("boston", Type.STRING_LEN)), tweets);
			DbIterator intermediate = Utility.applyOperator(ntd, filter2);
			
			
			Operator count = new Aggregate(intermediate, 0, 0, Aggregator.Op.COUNT);
			DbIterator counts = Utility.applyOperator(otd, count);

			Operator max = new Aggregate(counts, 1, -1, Aggregator.Op.MAX);
			DbIterator maxes = Utility.applyOperator(mtd, max);

			JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 0);
	        Join joinOp = new Join(p, counts, maxes);
	        
	        DbIterator output =  Utility.applyOperator(jtd, joinOp);
	        
	        
	        ArrayList<Integer> outFields = new ArrayList<Integer>();
	        outFields.add(0);
	        outFields.add(1);
	        ArrayList<Type> outTypes = new ArrayList<Type>();
	        outTypes.add(Type.STRING_TYPE);
	        outTypes.add(Type.INT_TYPE);
	        Project proj = new Project(outFields, outTypes, output);
	        
	        DbIterator results =  Utility.applyOperator(rtd, proj);
			
		}
		
		long timeend = System.currentTimeMillis();
    	
    	System.out.print("qwerty Metrics for Naive implementation: ");
    	System.out.println(timeend / 1000 - timemid / 1000);

	}

	@Test
	public void streamingTrendingTest() throws Exception {
		long timestart = System.currentTimeMillis();
		TupleDesc td = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.STRING_TYPE });
		TupleDesc mtd = new TupleDesc(new Type[] { Type.INT_TYPE });
		TupleDesc otd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE });
		TupleDesc jtd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE, Type.INT_TYPE });
		TupleDesc rtd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE });

		StreamReader isr = new FileStreamReader("trending.txt", td);

		Stream tweetStream = new Stream(isr);
		
		StreamToRelationTimeWindowConverter insertStreamConverter = new StreamToRelationTimeWindowConverter(tweetStream, 120, td);
		
		RelationToIstreamConverter rToSConverter = new RelationToIstreamConverter(rtd);

		long timemid = System.currentTimeMillis();
		for (int i = 0; i < 300; i++) {
			DbIterator tweets = insertStreamConverter.updateRelation();	
			Operator filter = new Filter(new Predicate(1, Op.EQUALS, new StringField("boston", Type.STRING_LEN)), tweets);
			DbIterator intermediate = Utility.applyOperator(td, filter);
			
			
			Operator count = new Aggregate(intermediate, 0, 0, Aggregator.Op.COUNT);
			DbIterator counts = Utility.applyOperator(otd, count);

			Operator max = new Aggregate(counts, 1, -1, Aggregator.Op.MAX);
			DbIterator maxes = Utility.applyOperator(mtd, max);

			JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 0);
	        Join joinOp = new Join(p, counts, maxes);
	        
	        DbIterator output =  Utility.applyOperator(jtd, joinOp);
	        
	        
	        ArrayList<Integer> outFields = new ArrayList<Integer>();
	        outFields.add(0);
	        outFields.add(1);
	        ArrayList<Type> outTypes = new ArrayList<Type>();
	        outTypes.add(Type.STRING_TYPE);
	        outTypes.add(Type.INT_TYPE);
	        Project proj = new Project(outFields, outTypes, output);
	        
	        DbIterator results =  Utility.applyOperator(rtd, proj);
	
			rToSConverter.updateStream(results);
			
		}
		
		long timeend = System.currentTimeMillis();
		
		System.out.print("qwerty Metrics for windowing implementation: ");
    	System.out.println(timeend / 1000 - timemid / 1000);
	}

}
