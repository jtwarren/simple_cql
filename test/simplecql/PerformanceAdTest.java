package simplecql;

import org.junit.Test;

import simpledb.DbIterator;
import simpledb.FileStreamReader;
import simpledb.Join;
import simpledb.JoinPredicate;
import simpledb.Predicate;
import simpledb.RelationToIstreamConverter;
import simpledb.Stream;
import simpledb.StreamReader;
import simpledb.StreamToRelationTimeWindowConverter;
import simpledb.TupleDesc;
import simpledb.Type;

public class PerformanceAdTest {

	@Test
	public void timeWindowSystemTest() throws Exception {
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
		
    	System.out.println(timemid / 1000 - timestart / 1000);
    	System.out.println(timeend / 1000 - timemid / 1000);
    	
//		Stream outputStream = rToSConverter.getStream();
		
//		for (int ts = 0; ts < 600; ts++) {
//			Tuple tuple = outputStream.getNext(ts);
//			while (tuple != null) {
//				System.out.println(tuple);
//				tuple = outputStream.getNext(ts);
//			}
//		}

	}

}
