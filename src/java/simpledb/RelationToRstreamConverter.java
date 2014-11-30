package simpledb;

import java.util.ArrayList;

public class RelationToRstreamConverter {
    private TupleDesc td;

    private DbIterator prevRelation;
    private ArrayList<Tuple> Rstream;

    public RelationToRstreamConverter(TupleDesc td) {
        this.td = td;

        prevRelation = null;
        Rstream = new ArrayList<Tuple>();
    }
    
    public void updateRstream(DbIterator nextRelation) throws DbException, TransactionAbortedException {
        ArrayList<Tuple> RstreamNew = new ArrayList<Tuple>();
        Tuple nextTuple = nextRelation.next();
        while (nextTuple != null) {
            RstreamNew.add(nextTuple);
            nextTuple = nextRelation.next();
        }
        Rstream = RstreamNew;
        prevRelation = nextRelation;
    }

    public DbIterator getRstream() {
        return new TupleIterator(td, Rstream);
    }
}
