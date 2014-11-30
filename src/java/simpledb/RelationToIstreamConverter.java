package simplecql;

import java.util.ArrayList;
import java.util.HashSet;

public class RelationToIstreamConverter {
    private TupleDesc td;

    private DbIterator prevRelation;
    private ArrayList<Tuple> Istream;

    public RelationToIstreamConverter(TupleDesc td) {
        this.td = td;

        prevRelation = null;
        Istream = new ArrayList<Tuple>();
    }
    
    public void updateIstream(DbIterator nextRelation) throws DbException, TransactionAbortedException {
        HashSet<Tuple> diff = new HashSet<Tuple>();
        Tuple prevTuple;
        if (prevRelation != null) { // only diff with prev TS if existed
            prevTuple = prevRelation.next();
            while (prevTuple != null) {
                diff.add(prevTuple);
                prevTuple = prevRelation.next();
            }
        }

        ArrayList<Tuple> IstreamNew = new ArrayList<Tuple>();
        Tuple nextTuple = nextRelation.next();
        while (nextTuple != null) {
            if (diff.contains(nextTuple)) {
                continue;
            }
            IstreamNew.add(nextTuple);
            nextTuple = nextRelation.next();
        }
        Istream = IstreamNew;
        prevRelation = nextRelation;
    }

    public DbIterator getIstream() {
        return new TupleIterator(td, Istream);
    }
}
