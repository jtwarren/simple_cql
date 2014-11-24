package simplecql;

import static org.junit.Assert.*;

import org.junit.Test;

import simpledb.FileStreamReader;
import simpledb.IntField;
import simpledb.StreamReader;
import simpledb.StringField;
import simpledb.TSField;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;

public class FileStreamReaderTest {
	
	// basic_test.txt looks like this:
	// a 0 0
	// b 1 1
	// c 2 2
	// d 3 3
	// e 4 4
	// f 5 4
	// g 6 4

	
    @Test public void fileTest() throws Exception {
    	TupleDesc td = new TupleDesc(new Type[]{Type.STRING_TYPE, Type.INT_TYPE, Type.TS_TYPE});
    	StreamReader sr = new FileStreamReader("basic_test.txt", td);
    	
    	Tuple tuple;
    	boolean correct;
    	
    	tuple = sr.getNext(0);
    	correct = tuple.getField(0).equals(new StringField("a", Type.STRING_LEN));
    	assertEquals(correct, true);
    	correct = tuple.getField(1).equals(new IntField(0));
    	assertEquals(correct, true);
    	correct = tuple.getField(2).equals(new TSField(0));
    	assertEquals(correct, true);
    	
    	tuple = sr.getNext(0);
    	assertNull(tuple);
    	
    	tuple = sr.getNext(1);
    	correct = tuple.getField(0).equals(new StringField("b", Type.STRING_LEN));
    	assertEquals(correct, true);
    	correct = tuple.getField(1).equals(new IntField(1));
    	assertEquals(correct, true);
    	correct = tuple.getField(2).equals(new TSField(1));
    	assertEquals(correct, true);
    	
    	tuple = sr.getNext(1);
    	assertNull(tuple);
    	
    	tuple = sr.getNext(2);
    	correct = tuple.getField(0).equals(new StringField("c", Type.STRING_LEN));
    	assertEquals(correct, true);
    	correct = tuple.getField(1).equals(new IntField(2));
    	assertEquals(correct, true);
    	correct = tuple.getField(2).equals(new TSField(2));
    	assertEquals(correct, true);
    	
    	tuple = sr.getNext(2);
    	assertNull(tuple);
    	
    	tuple = sr.getNext(3);
    	correct = tuple.getField(0).equals(new StringField("d", Type.STRING_LEN));
    	assertEquals(correct, true);
    	correct = tuple.getField(1).equals(new IntField(3));
    	assertEquals(correct, true);
    	correct = tuple.getField(2).equals(new TSField(3));
    	assertEquals(correct, true);
    	
    	tuple = sr.getNext(3);
    	assertNull(tuple);
    	
    	tuple = sr.getNext(4);
    	correct = tuple.getField(0).equals(new StringField("e", Type.STRING_LEN));
    	assertEquals(correct, true);
    	correct = tuple.getField(1).equals(new IntField(4));
    	assertEquals(correct, true);
    	correct = tuple.getField(2).equals(new TSField(4));
    	assertEquals(correct, true);
    	
    	tuple = sr.getNext(4);
    	correct = tuple.getField(0).equals(new StringField("f", Type.STRING_LEN));
    	assertEquals(correct, true);
    	correct = tuple.getField(1).equals(new IntField(5));
    	assertEquals(correct, true);
    	correct = tuple.getField(2).equals(new TSField(4));
    	assertEquals(correct, true);
    	
    	tuple = sr.getNext(4);
    	correct = tuple.getField(0).equals(new StringField("g", Type.STRING_LEN));
    	assertEquals(correct, true);
    	correct = tuple.getField(1).equals(new IntField(6));
    	assertEquals(correct, true);
    	correct = tuple.getField(2).equals(new TSField(4));
    	assertEquals(correct, true);
    	

    }

}
