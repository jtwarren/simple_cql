package simplecql;

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

public class Demo {
	
	public static void runLiveStreamWithAggregate() throws DbException, TransactionAbortedException, InterruptedException {
		int stepSize = 500;
		TupleDesc inputDesc = new TupleDesc(new Type[] {Type.STRING_TYPE});
		TupleDesc aggregateDesc = new TupleDesc(new Type[] {Type.STRING_TYPE, Type.INT_TYPE});
		LiveStreamReader sr = new LiveStreamReader(
				inputDesc,
				"scripts/output.txt",
				stepSize,
				10); // May want to play with this parameter to see if it makes a difference to performance
		sr.spawnReadingThread();
		
		Thread.sleep(2 * stepSize + 100);
		Stream inputStream = new Stream(sr);
		
		StreamToRelationTimeWindowConverter sToRConverter = new StreamToRelationTimeWindowConverter(inputStream, 0, inputDesc);
		RelationToIstreamConverter rToSConverter = new RelationToIstreamConverter(aggregateDesc);
		
		Stream outputStream = rToSConverter.getStream();
		
		int ts = 0;
		while (ts < 1000) {
			long startTime = System.currentTimeMillis();
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
			long endTime = System.currentTimeMillis();
			// System.out.println(String.format("Total execution time for timestep %d: %d ms", ts, (endTime - startTime)));
			ts++;
		}
	}

	public static void main(String[] args) {
		try {
			runLiveStreamWithAggregate();
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
