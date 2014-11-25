package simpledb;

import java.util.ArrayList;
import java.util.HashSet;

public class RelationToDstreamConverter {
    private TupleDesc td;

    private DbIterator prevRelation;
    private ArrayList<Tuple> Dstream;

    public RelationToDstreamConverter(TupleDesc td) {
        this.td = td;

        prevRelation = null;
        Dstream = new ArrayList<Tuple>();
    }
    
    public void updateDstream(DbIterator nextRelation) throws DbException, TransactionAbortedException {
        HashSet<Tuple> diff = new HashSet<Tuple>();
        Tuple nextTuple;
        if (nextRelation != null) { // only diff with prev TS if existed
            nextTuple = nextRelation.next();
            while (nextTuple != null) {
                diff.add(nextTuple);
                nextTuple = nextRelation.next();
            }
        }

        ArrayList<Tuple> DstreamNew = new ArrayList<Tuple>();
        Tuple prevTuple = prevRelation.next();
        while (prevTuple != null) {
            if (diff.contains(prevTuple)) {
                continue;
            }
            DstreamNew.add(prevTuple);
            prevTuple = prevRelation.next();
        }
        Dstream = DstreamNew;
        prevRelation = nextRelation;
    }

    public DbIterator getDstream() {
        return new TupleIterator(td, Dstream);
    }
}
