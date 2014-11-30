package simplecql;


import java.io.IOException;

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
import simpledb.StreamToRelationTupleWindowConverter;
import simpledb.TransactionAbortedException;
import simpledb.TupleDesc;
import simpledb.Type;
import simpledb.Predicate.Op;

public class SystemTest {

	@Test
	public void FileStreamWithAggregationTupleWindowTest() throws IOException, DbException, TransactionAbortedException {
		TupleDesc inputDesc = new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE});
		TupleDesc aggregateDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
		StreamReader sr = new FileStreamReader("aggregation_test.txt", inputDesc);
		
		Stream stream = new Stream(sr);
		StreamToRelationTupleWindowConverter sToRConverter = new StreamToRelationTupleWindowConverter(stream, 30, inputDesc);
		
		RelationToIstreamConverter rToSConverter = new RelationToIstreamConverter(aggregateDesc);
		
		// aggregation_test has data for 50 timesteps
		for (int i = 0; i < 50; i++) {
			DbIterator input = sToRConverter.updateRelation();
			Operator filter = new Filter(new Predicate(1, Op.GREATER_THAN_OR_EQ, new IntField(20)), input);
			DbIterator intermediate = Utility.applyOperator(inputDesc, filter);
			Operator aggregate = new Aggregate(intermediate, 1, -1, Aggregator.Op.AVG);
			DbIterator output = Utility.applyOperator(aggregateDesc, aggregate);
			rToSConverter.updateStream(output);
		}
		
		StreamReader expectedSr = new FileStreamReader("aggregation_test_output2.txt",
				new TupleDesc(new Type[]{Type.INT_TYPE}));
		Stream expectedStream = new Stream(expectedSr);
		
		Stream outputStream = rToSConverter.getStream();

		Utility.checkEquality(expectedStream, outputStream, 50);
	}
	
	@Test
	public void FileStreamWithAggregationTimeWindowTest() throws IOException, DbException, TransactionAbortedException {
		TupleDesc inputDesc = new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE});
		TupleDesc aggregateDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
		StreamReader sr = new FileStreamReader("aggregation_test.txt", inputDesc);
		
		Stream stream = new Stream(sr);
		StreamToRelationTimeWindowConverter sToRConverter = new StreamToRelationTimeWindowConverter(stream, 0, inputDesc);
		
		RelationToIstreamConverter rToSConverter = new RelationToIstreamConverter(aggregateDesc);

		// aggregation_test has data for 50 timesteps
		for (int i = 0; i < 50; i++) {
			DbIterator input = sToRConverter.updateRelation();
			Operator filter = new Filter(new Predicate(1, Op.GREATER_THAN_OR_EQ, new IntField(20)), input);
			DbIterator intermediate = Utility.applyOperator(inputDesc, filter);
			Operator aggregate = new Aggregate(intermediate, 1, -1, Aggregator.Op.AVG);
			DbIterator output = Utility.applyOperator(aggregateDesc, aggregate);
			rToSConverter.updateStream(output);
		}
		
		StreamReader expectedSr = new FileStreamReader("aggregation_test_output.txt",
				new TupleDesc(new Type[]{Type.INT_TYPE}));
		Stream expectedStream = new Stream(expectedSr);
		
		Stream outputStream = rToSConverter.getStream();

		Utility.checkEquality(expectedStream, outputStream, 50);
	}

}
