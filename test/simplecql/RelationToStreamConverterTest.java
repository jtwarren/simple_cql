package simplecql;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

import simpledb.DbException;
import simpledb.IntField;
import simpledb.RelationToDstreamConverter;
import simpledb.RelationToIstreamConverter;
import simpledb.RelationToRstreamConverter;
import simpledb.Stream;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.TupleIterator;
import simpledb.Type;

public class RelationToStreamConverterTest {

    @Test
    public void RelationToIstreamTest() 
                throws DbException, TransactionAbortedException {
		TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
        RelationToIstreamConverter converter = new RelationToIstreamConverter(td);

        // generate some relations for testing
        int numRelations = 5;
        TupleIterator[] relations = new TupleIterator[numRelations];
        for (int i = 0; i < numRelations; i++) {
            ArrayList<Tuple> relationTuples = new ArrayList<Tuple>();
            Tuple testTuple = new Tuple(td);
            testTuple.setField(0, new IntField(i));
            relationTuples.add(testTuple);
            relations[i] = new TupleIterator(td, relationTuples);
        }

        // actual conversions and checks
        Stream stream = converter.getStream();
        int ts = 0;
        for (TupleIterator relation : relations) {
            // run the relations through the converter and into the reader
            converter.updateStream(relation);

            relation.open();
            Tuple tupleIn = relation.next();

            // check the converter outputs what we expect for Istream
            // Read the stream to get tupleOuts
            Tuple tupleOut = stream.getNext(ts);
            assertTrue(tupleIn.getField(0) == tupleOut.getField(0));
            
            ts++;
            
        }
    }

    @Test
    public void RelationToDstreamTest() 
                throws DbException, TransactionAbortedException {
		TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
        RelationToDstreamConverter converter = new RelationToDstreamConverter(td);

        // generate tuples
        int numRelations = 5;
        Tuple[] tuples = new Tuple[numRelations];
        for (int t = 0; t < numRelations; t++) {
            Tuple testTuple = new Tuple(td);
            testTuple.setField(0, new IntField(t));
            tuples[t] = testTuple;
        }

        // now generate relations to test deletions over time
        TupleIterator[] relations = new TupleIterator[numRelations];
        for (int i = 0; i < numRelations; i++) {
            ArrayList<Tuple> relationTuples = new ArrayList<Tuple>();
            // start out with all tuples (5)
            // then for each increasing TS, remove the last tuple (4), etc
            for (int z = 0; z < numRelations - i; z++) {
                relationTuples.add(tuples[z]);
            }
            relations[i] = new TupleIterator(td, relationTuples);
        }

        // actual conversions and checks
        Stream stream = converter.getStream();
        for (int j = 0; j < numRelations; j++) {
            TupleIterator relation = relations[j];

            // run the relations through the converter and into the reader
            converter.updateStream(relation);

            // check the converter outputs what we expect for Dstream
            Tuple tupleIn;
            Tuple tupleOut;
            int endIndex = numRelations - j;
            if (endIndex < 5) {
                tupleIn = tuples[endIndex];
                tupleOut = stream.getNext(j);
                assertTrue(tupleIn.getField(0) == tupleOut.getField(0));
            }
        }

    }

    @Test
    public void RelationToRstreamTest() 
                throws DbException, TransactionAbortedException {
		TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
        RelationToRstreamConverter converter = new RelationToRstreamConverter(td);

        // generate tuples
        int numRelations = 5;
        Tuple[] tuples = new Tuple[numRelations];
        for (int t = 0; t < numRelations; t++) {
            Tuple testTuple = new Tuple(td);
            testTuple.setField(0, new IntField(t));
            tuples[t] = testTuple;
        }

        // now generate relations 
        TupleIterator[] relations = new TupleIterator[numRelations];
        for (int i = 0; i < numRelations; i++) {
            ArrayList<Tuple> relationTuples = new ArrayList<Tuple>();
            // start out with 1 tuple
            // then for each increasing TS, add more
            for (int z = 0; z <= i; z++) {
                relationTuples.add(tuples[z]);
            }
            relations[i] = new TupleIterator(td, relationTuples);
        }

        // actual conversions and checks
        Stream stream = converter.getStream();
        for (int j = 0; j < numRelations; j++) {
            TupleIterator relation = relations[j];

            // run the relations through the converter and into the reader
            converter.updateStream(relation);

            // check the converter outputs what we expect for Rstream
            relation.open();
            Tuple tupleIn;
            Tuple tupleOut;
            for (int k = 0; k <= j; k++) {
                tupleIn = relation.next();
                tupleOut = stream.getNext(j);
                assertTrue(tupleIn.getField(0) == tupleOut.getField(0));
            }
            assertFalse(relation.hasNext());
        }
    }
}
