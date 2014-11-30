package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

public class RelationStreamReader implements StreamReader {
    private int internal_ts;
    private TupleDesc td;
    private HashMap<Integer, ArrayList<Tuple>> timesteps;
    private HashMap<Integer, Integer> currIndices;
    
    private int ts;

    public RelationStreamReader(TupleDesc td) {
        this.internal_ts = 0;
        this.td = td;
        this.timesteps = new HashMap<Integer, ArrayList<Tuple>>();
        this.currIndices = new HashMap<Integer, Integer>();
        
        ts = 0;
    }

    public void updateStream(DbIterator nextStream) throws DbException, TransactionAbortedException {
        if (nextStream != null) {
            nextStream.open();
            Tuple insert;
            while (nextStream.hasNext()) {
                insert = nextStream.next();
                if (!timesteps.containsKey(internal_ts)) {
                    timesteps.put(internal_ts, new ArrayList<Tuple>());
                    currIndices.put(internal_ts, 0);
                }
                timesteps.get(internal_ts).add(insert);
            }
        }
        internal_ts++;
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
