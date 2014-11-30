package simpledb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class FileStreamReader implements StreamReader {
	
	private ArrayList<Tuple> tuples;
	private int currentIndex;
	private TupleDesc td;
	private HashMap<Integer, Integer> startTimes;
	private HashMap<Integer, Integer> endTimes;
	private boolean returnNullNext = false;
	
	public FileStreamReader(String filename, TupleDesc td) throws IOException {
		this.tuples = new ArrayList<Tuple>();
		startTimes = new HashMap<Integer, Integer> ();
		endTimes = new HashMap<Integer, Integer> ();
		
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line;
		int lastStartTimeRecorded = -1;
		Tuple lastTuple = null;
		int lastTs = -1;
		int cnt = 0;
		while ((line = br.readLine()) != null) {
			Tuple newTuple = makeTuple(line, td);
			int newTs = getTs(line);
			tuples.add(newTuple);
			if (newTs > lastStartTimeRecorded) {
				startTimes.put(newTs, cnt);
				lastStartTimeRecorded = newTs;
				if (lastTuple != null) {
					endTimes.put(lastTs, cnt - 1);
				}
			}
			lastTuple = newTuple;
			lastTs = newTs;
			cnt++;
		}
		if (lastTuple != null) {
			endTimes.put(lastTs, cnt - 1);
		}
		br.close();
		
		currentIndex = 0;
		this.td = td;
	}

	@Override
	public Tuple getNext(int ts) {
		if (returnNullNext) {
			returnNullNext = false;
			return null;
		}
		if (!(currentIndex >= startTimes.get(ts) && currentIndex <= endTimes.get(ts))) {
			currentIndex = startTimes.get(ts);
		}
		if (currentIndex == endTimes.get(ts)) {
			returnNullNext = true;
		}
		Tuple currentTuple = tuples.get(currentIndex);
		currentIndex++;
		return currentTuple;
	}
	
	private Tuple makeTuple(String str, TupleDesc td) {
		String[] splitted = str.split("\\s+");
		
		Tuple tuple = new Tuple(td);
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
