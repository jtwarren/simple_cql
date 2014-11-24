package simpledb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class FileStreamReader implements StreamReader {
	
	private ArrayList<Tuple> tuples;
	private int currentIndex;
	
	public FileStreamReader(String filename, TupleDesc td) throws Exception {
		this.tuples = new ArrayList<Tuple>();
		
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line;
		while ((line = br.readLine()) != null) {
			this.tuples.add(makeTuple(line, td));
		}
		br.close();
		
		currentIndex = 0;
	}

	@Override
	public Tuple getNext(int ts) {
		Tuple currentTuple = tuples.get(currentIndex);
		TSField tsField = (TSField) currentTuple.getField(currentTuple.getTupleDesc().numFields() - 1);
		
		if (tsField.getValue() == ts) {
			this.currentIndex += 1;
			return currentTuple;
		}
		
		return null;
	}
	
	private Tuple makeTuple(String str, TupleDesc td) throws Exception {
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
				throw new Exception("Type not supported for file reading");
			}
		}
		
		return tuple;
	}

}
