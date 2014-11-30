package simpledb;

import java.util.ArrayList;

public class RelationToRstreamConverter implements RelationToStreamConverter {
    private TupleDesc td;

    private ArrayList<Tuple> Rstream;
    
    private RelationStreamReader reader;

    public RelationToRstreamConverter(TupleDesc td) {
        this.td = td;

        Rstream = new ArrayList<Tuple>();
        reader = new RelationStreamReader(td);
    }
    
    public void updateStream(DbIterator nextRelation) throws DbException, TransactionAbortedException {
        ArrayList<Tuple> RstreamNew = new ArrayList<Tuple>();
        Tuple nextTuple = nextRelation.next();
        while (nextTuple != null) {
            RstreamNew.add(nextTuple);
            nextTuple = nextRelation.next();
        }
        Rstream = RstreamNew;
        
        reader.updateStream(new TupleIterator(td, Rstream));
    }

    public Stream getStream() {
        return new Stream(reader);
    }
}
