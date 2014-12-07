package simplecql;

import org.junit.Test;

import simpledb.Aggregate;
import simpledb.Aggregator;
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
import simpledb.StringField;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;
import simpledb.Predicate.Op;

public class PerformanceErrorTest {

	@Test
	public void timeWindowSystemTest() throws Exception {
		long timestart = System.currentTimeMillis();
		TupleDesc td = new TupleDesc(new Type[] { Type.STRING_TYPE });
		TupleDesc otd = new TupleDesc(new Type[] { Type.INT_TYPE });

		StreamReader isr = new FileStreamReader("error_log.txt", td);

		Stream logStream = new Stream(isr);
		
		StreamToRelationTimeWindowConverter insertStreamConverter = new StreamToRelationTimeWindowConverter(logStream, 5, td);
		
		RelationToIstreamConverter rToSConverter = new RelationToIstreamConverter(otd);

    	long timemid = System.currentTimeMillis();
		for (int i = 0; i < 1800; i++) {
			DbIterator messages = insertStreamConverter.updateRelation();	
			
			Operator filter = new Filter(new Predicate(0, Op.EQUALS, new StringField("server_500", Type.STRING_LEN)), messages);
			DbIterator intermediate = Utility.applyOperator(td, filter);
			
			Operator aggregate = new Aggregate(intermediate, 0, -1, Aggregator.Op.COUNT);
			
			DbIterator output = Utility.applyOperator(otd, aggregate);
			
			rToSConverter.updateStream(output);
			
		}
		
		long timeend = System.currentTimeMillis();
		
    	System.out.println(timemid / 1000 - timestart / 1000);
    	System.out.println(timeend / 1000 - timemid / 1000);

	}
}
