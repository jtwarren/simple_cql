package simplecql;

import org.junit.Test;

import simpledb.Aggregate;
import simpledb.Aggregator;
import simpledb.DbIterator;
import simpledb.FileStreamReader;
import simpledb.Filter;
import simpledb.Operator;
import simpledb.Predicate;
import simpledb.RelationToIstreamConverter;
import simpledb.Stream;
import simpledb.StreamReader;
import simpledb.StreamToRelationTimeWindowConverter;
import simpledb.StringField;
import simpledb.TupleDesc;
import simpledb.Type;
import simpledb.Predicate.Op;

public class SimpleErrorTest {

	@Test
	public void timeWindowSystemTest() throws Exception {
		TupleDesc td = new TupleDesc(new Type[] { Type.STRING_TYPE });
		TupleDesc otd = new TupleDesc(new Type[] { Type.INT_TYPE });

		StreamReader isr = new FileStreamReader("simple_error_log.txt", td);

		Stream logStream = new Stream(isr);
		
		StreamToRelationTimeWindowConverter insertStreamConverter = new StreamToRelationTimeWindowConverter(logStream, 0, td);
		
		RelationToIstreamConverter rToSConverter = new RelationToIstreamConverter(otd);

		for (int i = 0; i < 5; i++) {
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

		Utility.checkEquality(expectedStream, outputStream, 5);
	}
}
