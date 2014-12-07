package simplecql;

import org.junit.Test;

import simpledb.Aggregate;
import simpledb.Aggregator;
import simpledb.DbIterator;
import simpledb.FileStreamReader;
import simpledb.Filter;
import simpledb.Join;
import simpledb.JoinPredicate;
import simpledb.Operator;
import simpledb.Predicate;
import simpledb.RelationToIstreamConverter;
import simpledb.Stream;
import simpledb.StreamReader;
import simpledb.StreamToRelationTimeWindowConverter;
import simpledb.StringField;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;
import simpledb.Predicate.Op;

public class SimpleTrendingTest {

	@Test
	public void streamingErrorTest() throws Exception {
		TupleDesc td = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.STRING_TYPE });
		TupleDesc mtd = new TupleDesc(new Type[] { Type.INT_TYPE });
		TupleDesc otd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE });
		TupleDesc jtd = new TupleDesc(new Type[] { Type.STRING_TYPE, Type.INT_TYPE, Type.INT_TYPE });

		StreamReader isr = new FileStreamReader("simple_trending.txt", td);

		Stream tweetStream = new Stream(isr);
		
		StreamToRelationTimeWindowConverter insertStreamConverter = new StreamToRelationTimeWindowConverter(tweetStream, 0, td);
		
		RelationToIstreamConverter rToSConverter = new RelationToIstreamConverter(otd);

		for (int i = 0; i < 5; i++) {
			DbIterator tweets = insertStreamConverter.updateRelation();	
//			
			Operator filter = new Filter(new Predicate(1, Op.EQUALS, new StringField("boston", Type.STRING_LEN)), tweets);
			DbIterator intermediate = Utility.applyOperator(td, filter);
			
			
			Operator count = new Aggregate(intermediate, 0, 0, Aggregator.Op.COUNT);
			DbIterator counts = Utility.applyOperator(otd, count);

			Operator max = new Aggregate(counts, 1, -1, Aggregator.Op.MAX);
			DbIterator maxes = Utility.applyOperator(mtd, max);
			
			System.out.println(maxes.getTupleDesc());
			System.out.println(counts.getTupleDesc());
			
//			System.out.println("------------");
//			counts.open();
//			while(counts.hasNext()) {
//				System.out.println(counts.next());
//			}
//			
//			System.out.println("-");
//			maxes.open();
//			while(maxes.hasNext()) {
//				System.out.println(maxes.next());
//			}
			

			JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 0);
	        Join joinOp = new Join(p, counts, maxes);
	        
	        
	        DbIterator output =  Utility.applyOperator(jtd, joinOp);
			
//			System.out.println("---");
//			joinOp.open();
//			while(joinOp.hasNext()) {
//				System.out.println(joinOp.next());
//			}
			

			
//			DbIterator output = Utility.applyOperator(otd, max);
//			
//			rToSConverter.updateStream(output);
			
		}
		
//		Stream outputStream = rToSConverter.getStream();
//		
//		for (int i = 0; i < 5; i++) {
//			Tuple tuple = outputStream.getNext(i);
//			while(tuple != null) {
//				System.out.print(i);
//				System.out.println(tuple);
//				tuple = outputStream.getNext(i);
//			}
//		}
		
//		StreamReader expectedSr = new FileStreamReader("simple_error_log_output.txt", otd);
//		Stream expectedStream = new Stream(expectedSr);
//		Stream outputStream = rToSConverter.getStream();
//
//		Utility.checkEquality(expectedStream, outputStream, 6);
	}
}
