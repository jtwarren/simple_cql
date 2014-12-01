package simpledb;

import java.util.ArrayList;
import java.util.HashSet;

public class RelationToIstreamConverter implements RelationToStreamConverter {
    private TupleDesc td;

    private DbIterator prevRelation;
    private ArrayList<Tuple> Istream;
    
    private RelationStreamReader reader;

    public RelationToIstreamConverter(TupleDesc td) {
        this.td = td;

        prevRelation = null;
        Istream = new ArrayList<Tuple>();
        
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

        ArrayList<Tuple> IstreamNew = new ArrayList<Tuple>();
        nextRelation.open();;
        ArrayList<Tuple> tuples = new ArrayList<Tuple> ();
        while (nextRelation.hasNext()) {
            Tuple nextTuple = nextRelation.next();
            tuples.add(nextTuple);
            if (diff.contains(nextTuple)) {
                continue;
            }
            IstreamNew.add(nextTuple);
        }
        nextRelation.close();
        Istream = IstreamNew;

        prevRelation = new TupleIterator(this.td, tuples);
        
        reader.updateStream(new TupleIterator(td, Istream));
    }

    public Stream getStream() {
        return new Stream(reader);
    }
}
