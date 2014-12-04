package simplecql;

import java.io.IOException;

import org.junit.Test;

import simpledb.Aggregate;
import simpledb.Aggregator;
import simpledb.DbException;
import simpledb.DbIterator;
import simpledb.IntField;
import simpledb.LiveStreamReader;
import simpledb.Operator;
import simpledb.RelationToIstreamConverter;
import simpledb.Stream;
import simpledb.StreamToRelationTimeWindowConverter;
import simpledb.StringField;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;

public class LiveStreamReaderTest {

	@Test
	public void test() throws IOException, InterruptedException, DbException, TransactionAbortedException {
		int stepSize = 500;
		TupleDesc inputDesc = new TupleDesc(new Type[] {Type.STRING_TYPE});
		TupleDesc aggregateDesc = new TupleDesc(new Type[] {Type.STRING_TYPE, Type.INT_TYPE});
		LiveStreamReader sr = new LiveStreamReader(inputDesc, "scripts/output.txt", stepSize);
		sr.spawnReadingThread();
		
		Thread.sleep(2 * stepSize + 100);
		Stream inputStream = new Stream(sr);
		
		StreamToRelationTimeWindowConverter sToRConverter = new StreamToRelationTimeWindowConverter(inputStream, 0, inputDesc);
		RelationToIstreamConverter rToSConverter = new RelationToIstreamConverter(aggregateDesc);
		
		Stream outputStream = rToSConverter.getStream();
		
		for (int ts = 0; ts < 50; ts++) {
			DbIterator input = null;
			try {
				input = sToRConverter.updateRelation();
			} catch (RuntimeException e) {
				ts--;
				continue;
			}
			Operator aggregate = new Aggregate(input, 0, 0, Aggregator.Op.COUNT);
			DbIterator output = Utility.applyOperator(aggregateDesc, aggregate);
			rToSConverter.updateStream(output);
			
			Tuple outputTuple = outputStream.getNext(ts);
			System.out.println("__________________________________________");
			System.out.println(String.format("Printing results for time %d", ts));
			System.out.println("__________________________________________");
			while (outputTuple != null) {
				System.out.println(String.format("%s: %d",
						((StringField) outputTuple.getField(0)).getValue(),
						((IntField) outputTuple.getField(1)).getValue()));
				outputTuple = outputStream.getNext(ts);
			}
		}
	}

}
