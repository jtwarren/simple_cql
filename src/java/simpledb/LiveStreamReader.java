package simpledb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class LiveStreamReader implements StreamReader {
	
	private TupleDesc td;
	private File file;
	private long ptr;
	private int ts;
	
	private int tsNext;
	private Iterator<Tuple> iterator;
	
	private HashMap<Integer, ArrayList<Tuple>> tuples;
	
	public LiveStreamReader(TupleDesc td, String filename) {
		this.td = td;

		file = new File(filename);
		ptr = file.length();
		ts = 0;
		tsNext = -1;
		tuples = new HashMap<Integer, ArrayList<Tuple>> ();
	}
	
	private Tuple makeTuple(String str) {
		String[] splitted = str.split(",\\s+");
		
		Tuple tuple = new Tuple(td);
		for (int i = 0; i < splitted.length; i++) {
			Type type = td.getFieldType(i);
			if (type == Type.STRING_TYPE) {
				tuple.setField(i, new StringField(splitted[i], Type.STRING_LEN));
			} else if(type == Type.INT_TYPE) {
				tuple.setField(i, new IntField(Integer.parseInt(splitted[i])));
			} else {
				throw new RuntimeException("Type not supported for file reading");
			}
		}
		
		return tuple;
	}
	
	public void spawnReadingThread() {
		Runnable r = new Runnable() {
	         public void run() {
	             try {
					read();
				} catch (IOException e) {
					Thread.currentThread().interrupt();
				}
	         }
	     };

	     new Thread(r).start();
	}
	
	public void read() throws IOException {
		// TODO: Implement garbage collection here
		while (true) {
			try {
				Thread.sleep(5000); // Sleep for 5 seconds
				if (ptr == file.length()) {
					continue;
				}
				RandomAccessFile raf = new RandomAccessFile(file, "r");
				raf.seek(ptr);
				String line = null;
				ArrayList<Tuple> currentTimestampTuples = new ArrayList<Tuple> ();
				while ((line = raf.readLine()) != null) {
					Tuple tuple = makeTuple(line);
					currentTimestampTuples.add(tuple);
				}
				synchronized(this) {
					tuples.put(ts, currentTimestampTuples);
				}
				ptr = raf.getFilePointer();
				raf.close();
			} catch (InterruptedException e) {
				throw new RuntimeException("Sleep interrupted");
			}
			ts++;
		}
	}

	@Override
	public Tuple getNext(int ts) {
		if (ts != tsNext) {
			synchronized(this) {
				if (tuples.containsKey(ts + 1)) {
					tsNext = ts;
					iterator = tuples.get(ts).iterator();
				} else {
					throw new RuntimeException("ts too far ahead!");
				}
			}
		}
		if (iterator.hasNext()) {
			return iterator.next();
		}
		return null;
	}

	@Override
	public TupleDesc getTupleDesc() {
		return td;
	}

}
