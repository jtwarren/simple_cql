package simpledb;

import java.util.ArrayList;
import java.util.HashSet;

public class RelationToIstreamConverter implements RelationToStreamConverter {
    private TupleDesc td;

    private DbIterator prevRelation;
    private ArrayList<Tuple> Istream;
    
    private IstreamReader reader;

    public RelationToIstreamConverter(TupleDesc td) {
        this.td = td;

        prevRelation = null;
        Istream = new ArrayList<Tuple>();
        
        reader = new IstreamReader(td);
    }
    
    public void updateStream(DbIterator nextRelation) throws DbException, TransactionAbortedException {
        HashSet<Tuple> diff = new HashSet<Tuple>();
        Tuple prevTuple;
        if (prevRelation != null) { // only diff with prev TS if existed
            prevRelation.rewind();
            while (prevRelation.hasNext()) {
                prevTuple = prevRelation.next();
                diff.add(prevTuple);
            }
        }

        ArrayList<Tuple> IstreamNew = new ArrayList<Tuple>();
        nextRelation.open();
        Tuple nextTuple;
        while (nextRelation.hasNext()) {
            nextTuple = nextRelation.next();
            if (diff.contains(nextTuple)) {
                continue;
            }
            IstreamNew.add(nextTuple);
        }
        Istream = IstreamNew;
        prevRelation = nextRelation;
        
        reader.updateStream(new TupleIterator(td, Istream));
    }

    public Stream getStream() {
        return new Stream(reader);
    }
}
