package simpledb;

import java.util.ArrayList;
import java.util.HashSet;

public class RelationToDstreamConverter implements RelationToStreamConverter {
    private TupleDesc td;

    private DbIterator prevRelation;
    private ArrayList<Tuple> Dstream;
    
    private RelationStreamReader reader;

    public RelationToDstreamConverter(TupleDesc td) {
        this.td = td;

        prevRelation = null;
        Dstream = new ArrayList<Tuple>();

        reader = new RelationStreamReader(td);
    }
    
    public void updateStream(DbIterator nextRelation) throws DbException, TransactionAbortedException {
        HashSet<Tuple> diff = new HashSet<Tuple>();
        nextRelation.open();
        Tuple nextTuple;
        while (nextRelation.hasNext()) {
            nextTuple = nextRelation.next();
            diff.add(nextTuple);
        }

        if (prevRelation != null) { // only diff with prev TS if existed
            ArrayList<Tuple> DstreamNew = new ArrayList<Tuple>();
            prevRelation.rewind();
            Tuple prevTuple; 
            while (prevRelation.hasNext()) {
                prevTuple = prevRelation.next();
                if (diff.contains(prevTuple)) {
                    continue;
                }
                DstreamNew.add(prevTuple);
            }
            Dstream = DstreamNew;
        }
        prevRelation = nextRelation;
        
        reader.updateStream(new TupleIterator(td, Dstream));
    }

    public Stream getStream() {
        return new Stream(reader);
    }
}
