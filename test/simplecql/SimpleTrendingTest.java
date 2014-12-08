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

public class SimpleTrendingTest {

	@Test
	public void streamingTrendingTest() throws Exception {
		TupleDesc td = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.STRING_TYPE });
		TupleDesc mtd = new TupleDesc(new Type[] { Type.INT_TYPE });
		TupleDesc otd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE });
		TupleDesc jtd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE, Type.INT_TYPE });
		TupleDesc rtd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE });

		StreamReader isr = new FileStreamReader("simple_trending.txt", td);

		Stream tweetStream = new Stream(isr);
		
		StreamToRelationTimeWindowConverter insertStreamConverter = new StreamToRelationTimeWindowConverter(tweetStream, 120, td);
		
		RelationToIstreamConverter rToSConverter = new RelationToIstreamConverter(rtd);

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
		
		StreamReader expectedSr = new FileStreamReader("simple_trending_output.txt", rtd);
		Stream expectedStream = new Stream(expectedSr);
		Stream outputStream = rToSConverter.getStream();

		Utility.checkEquality(expectedStream, outputStream, 5);
	}
}
