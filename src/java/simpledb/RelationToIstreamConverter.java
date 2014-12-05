package simpledb;

import java.util.ArrayList;
import java.util.HashSet;

public class RelationToIstreamConverter implements RelationToStreamConverter {
    private TupleDesc td;

    private DbIterator prevRelation;
    private ArrayList<Tuple> iStream;
    
    private RelationStreamReader reader;

    public RelationToIstreamConverter(TupleDesc td) {
        this.td = td;

        prevRelation = null;
        iStream = new ArrayList<Tuple>();
        
        reader = new RelationStreamReader(td);
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

        ArrayList<Tuple> iStreamNew = new ArrayList<Tuple>();
        nextRelation.open();;
        ArrayList<Tuple> tuples = new ArrayList<Tuple> ();
        while (nextRelation.hasNext()) {
            Tuple nextTuple = nextRelation.next();
            tuples.add(nextTuple);
            if (diff.contains(nextTuple)) {
                continue;
            }
            iStreamNew.add(nextTuple);
        }
        nextRelation.close();
        iStream = iStreamNew;

        prevRelation = new TupleIterator(this.td, tuples);
        
        reader.updateStream(new TupleIterator(td, iStream));
    }

    public Stream getStream() {
        return new Stream(reader);
    }
}
