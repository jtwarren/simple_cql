package simplecql;

import java.io.IOException;

import org.junit.Test;

import simpledb.LiveStreamReader;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;

public class LiveStreamReaderTest {

	@Test
	public void test() throws IOException, InterruptedException {
		LiveStreamReader sr = new LiveStreamReader(
				new TupleDesc(new Type[] {Type.STRING_TYPE}), "scripts/output.txt");
		sr.spawnReadingThread();
		
		Thread.sleep(11000);
		for (int ts = 0; ts < 50; ts++) {
			Thread.sleep(5000);
			Tuple nextTuple = sr.getNext(ts);
			while (nextTuple != null) {
				System.out.println(nextTuple);
				nextTuple = sr.getNext(ts);
			}
		}
	}

}
