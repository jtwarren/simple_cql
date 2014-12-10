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

public class SimpleAdTest {

	@Test
	public void streamingAdTest() throws Exception {
		TupleDesc itd = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE });
		TupleDesc etd = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE });
		TupleDesc jtd = new TupleDesc(new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE });

		StreamReader isr = new FileStreamReader("simple_ad_insert.txt", itd);
		StreamReader est = new FileStreamReader("simple_ad_event.txt", etd);

		Stream insertStream = new Stream(isr);
		Stream eventStream = new Stream(est);
		
		StreamToRelationTimeWindowConverter insertStreamConverter = new StreamToRelationTimeWindowConverter(insertStream, 10, itd);
		StreamToRelationTimeWindowConverter eventStreamConverter = new StreamToRelationTimeWindowConverter(eventStream, 10, etd);
		
		RelationToIstreamConverter rToSConverter = new RelationToIstreamConverter(jtd);

		for (int i = 0; i < 10; i++) {
			DbIterator insertions = insertStreamConverter.updateRelation();
			DbIterator events = eventStreamConverter.updateRelation();			
			
			JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 1);
	        Join joinOp = new Join(p, insertions, events);
	        
	        DbIterator output = Utility.applyOperator(jtd, joinOp);
			
			rToSConverter.updateStream(output);
			
		}
		
		StreamReader expectedSr = new FileStreamReader("simple_ad_insert_event_output.txt", jtd);
		Stream expectedStream = new Stream(expectedSr);
		Stream outputStream = rToSConverter.getStream();

		Utility.checkEquality(expectedStream, outputStream, 10);
	}
}
