package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

public class IstreamReader implements StreamReader {
    private TupleDesc td;
    private HashMap<Integer, ArrayList<Tuple>> timesteps;
    private HashMap<Integer, Integer> currIndices;
    
    private int ts;

    public IstreamReader(TupleDesc td) {
        this.td = td;
        this.timesteps = new HashMap<Integer, ArrayList<Tuple>>();
        this.currIndices = new HashMap<Integer, Integer>();
        
        ts = 0;
    }

    public void updateStream(DbIterator Istream) throws DbException, TransactionAbortedException {
        Istream.open();
        Tuple insert;
        while (Istream.hasNext()) {
            insert = Istream.next();
            if (!timesteps.containsKey(ts)) {
                timesteps.put(ts, new ArrayList<Tuple>());
                currIndices.put(ts, 0);
            }
            timesteps.get(ts).add(insert);
        }
        
        ts++;
    }

    public Tuple getNext(int ts) {
        Tuple nextTuple = null;
        if (currIndices.containsKey(ts)) {
            int currIndex = currIndices.get(ts);
            if (currIndex < timesteps.get(ts).size()) {
                nextTuple = timesteps.get(ts).get(currIndex);
                currIndices.put(ts, currIndex + 1);
            }
        }
        return nextTuple;
    }

    public void resetCurrIndex(int ts) {
        if (currIndices.containsKey(ts)) {
            currIndices.put(ts, 0);
        }
    }

    public TupleDesc getTupleDesc() {
        return td;
    }
}
