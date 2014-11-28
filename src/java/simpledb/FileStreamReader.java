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
		int cnt = 0;
		while ((line = br.readLine()) != null) {
			Tuple newTuple = makeTuple(line, td);
			tuples.add(newTuple);
			TSField tsField = (TSField) newTuple.getField(newTuple.getTupleDesc().numFields() - 1);
			if (tsField.getValue() > lastStartTimeRecorded) {
				startTimes.put(tsField.getValue(), cnt);
				lastStartTimeRecorded = tsField.getValue();
				if (lastTuple != null) {
					TSField lasttsField = (TSField) lastTuple.getField(lastTuple.getTupleDesc().numFields() - 1);
					endTimes.put(lasttsField.getValue(), cnt - 1);
				}
			}
			lastTuple = newTuple;
			cnt++;
		}
		if (lastTuple != null) {
			TSField lasttsField = (TSField) lastTuple.getField(lastTuple.getTupleDesc().numFields() - 1);
			endTimes.put(lasttsField.getValue(), cnt - 1);
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
		for (int i = 0; i < splitted.length; i++) {
			Type type = td.getFieldType(i);
			if (type == Type.STRING_TYPE) {
				tuple.setField(i, new StringField(splitted[i], Type.STRING_LEN));
				continue;
			} else if(type == Type.INT_TYPE) {
				tuple.setField(i, new IntField(Integer.parseInt(splitted[i])));
				continue;
			} else if (type == Type.TS_TYPE) {
				tuple.setField(i, new TSField(Integer.parseInt(splitted[i])));
				continue;
			} else {
				throw new RuntimeException("Type not supported for file reading");
			}
		}
		
		return tuple;
	}

	@Override
	public TupleDesc getTupleDesc() {
		return td;
	}

}
