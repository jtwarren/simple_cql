package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

public class DstreamReader implements StreamReader {
    private HashMap<Integer, ArrayList<Tuple>> timesteps;
    private HashMap<Integer, Integer> currIndices;
    private TupleDesc td;

    public DstreamReader(TupleDesc td) {
        this.td = td;
        this.timesteps = new HashMap<Integer, ArrayList<Tuple>>();
    }

    public void updateStream(DbIterator Dstream) throws DbException, TransactionAbortedException {
        Tuple delete = Dstream.next();
        while (delete != null) {
            if (!delete.hasTimeStamp()) {
                throw new RuntimeException("Tuple in DstreamReader does not have time stamp");
            }
            int ts = ((TSField) delete.getTimeStamp()).getValue();
            if (!timesteps.containsKey(ts)) {
                timesteps.put(ts, new ArrayList<Tuple>());
                currIndices.put(ts, 0);
            }
            timesteps.get(ts).add(delete);
            delete = Dstream.next();
        }
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
