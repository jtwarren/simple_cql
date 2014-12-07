package simpledb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class FileStreamReader implements StreamReader {
	
	private Iterator<Tuple> iterator;
	private int ts;

	private TupleDesc td;
	private HashMap<Integer, ArrayList<Tuple>> tuples;
	
	private int counter = 0;
	
	public FileStreamReader(String filename, TupleDesc td) throws IOException {
		this.tuples = new HashMap<Integer, ArrayList<Tuple>> ();
		
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line;
		while ((line = br.readLine()) != null) {
			Tuple newTuple = makeTuple(line, td);
			int newTs = getTs(line);
			if (!tuples.containsKey(newTs))
				tuples.put(newTs, new ArrayList<Tuple> ());
			tuples.get(newTs).add(newTuple);
		}
		br.close();
		
		ts = -1;
		this.td = td;
	}

	@Override
	public Tuple getNext(int ts) {
		if (this.ts != ts) {
			this.ts = ts;
			if (!tuples.containsKey(ts)) {
				return null;
			}
			iterator = tuples.get(ts).iterator();
		}
		if (!iterator.hasNext()) {
			this.ts = -1;
			return null;
		}
		return iterator.next();
	}
	
	private Tuple makeTuple(String str, TupleDesc td) {
		String[] splitted = str.split("\\s+");
		
		Tuple tuple = new Tuple(td);
		tuple.setRecordId(new RecordId(null, counter++));
		// Last column represents the timestamp
		for (int i = 0; i < splitted.length - 1; i++) {
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
	
	private int getTs(String str) {
		String[] splitted = str.split("\\s+");
		return Integer.parseInt(splitted[splitted.length - 1]);
	}

	@Override
	public TupleDesc getTupleDesc() {
		return td;
	}

}
