package simplecql;

import static org.junit.Assert.*;

import org.junit.Test;

import simpledb.IntField;
import simpledb.SimpleStreamReader;
import simpledb.Tuple;

public class SimpleStreamReaderTest {

	@Test
	public void simpleTest() {
    	SimpleStreamReader sr = new SimpleStreamReader();
    	
    	// 10 iterations
    	for (int i = 0; i < 10; i++) {
    		Tuple tuple = sr.addTuple();
    		IntField field;
    		int start = -1;
    		int end = -1;
    		boolean isFirst = true;
    		while (tuple != null) {
    			field = (IntField) tuple.getField(0);
    			if (isFirst) {
    				start = field.getValue();
    				isFirst = false;
    			}
    			end = field.getValue();
    			tuple = sr.addTuple();
    		}
    		
    		tuple = sr.getNext(i);
    		while (tuple != null) {
    			assertEquals(start, ((IntField) tuple.getField(0)).getValue());
    			tuple = sr.getNext(i);
    			if (tuple != null)
    				start++;
    		}
    		assertEquals(end, start);
    	}
	}

}
