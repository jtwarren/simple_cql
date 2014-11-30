package simplecql;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

import simpledb.DbException;
import simpledb.IntField;
import simpledb.IstreamReader;
import simpledb.RelationToIstreamConverter;
import simpledb.TransactionAbortedException;
import simpledb.TSField;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.TupleIterator;
import simpledb.Type;

public class RelationToStreamConverterTest {

    @Test
    public void RelationToIstreamTest() 
                throws DbException, TransactionAbortedException {
		TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE, Type.TS_TYPE});
        RelationToIstreamConverter converter = new RelationToIstreamConverter(td);
        IstreamReader reader = new IstreamReader(td);

        // generate some relations for testing
        int numRelations = 5;
        TupleIterator[] relations = new TupleIterator[numRelations];
        for (int i = 0; i < numRelations; i++) {
            ArrayList<Tuple> relationTuples = new ArrayList<Tuple>();
            Tuple testTuple = new Tuple(td);
            testTuple.setField(0, new IntField(i));
            testTuple.setField(1, new TSField(i));
            relationTuples.add(testTuple);
            relations[i] = new TupleIterator(td, relationTuples);
        }

        // actual conversions and checks
        for (TupleIterator relation : relations) {
            // run the relations through the converter and into the reader
            converter.updateIstream(relation);
            TupleIterator Istream = (TupleIterator) converter.getIstream();
            reader.updateStream(Istream);

            relation.open();
            Tuple tupleIn = relation.next();
            Tuple tupleOut;

            // check the converter outputs what we expect for Istream
            Istream.open();
            tupleOut = Istream.next();
            assertTrue(tupleIn.getField(0) == tupleOut.getField(0));
            assertTrue(tupleIn.getField(1) == tupleOut.getField(1));

            // check the reader receives what we expect
            tupleOut = reader.getNext(((TSField) tupleIn.getField(1)).getValue());
            assertTrue(tupleIn.getField(0) == tupleOut.getField(0));
            assertTrue(tupleIn.getField(1) == tupleOut.getField(1));
            
        }
    }

    //@Test
    //public void RelationToDstreamTest() {
    //    RelationToDstreamConverter 

    //}

    //@Test
    //public void RelationToRstreamTest() {
    //    RelationToRstreamConverter 

    //}
}
