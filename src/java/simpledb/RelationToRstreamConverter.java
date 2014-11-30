package simpledb;

import java.util.ArrayList;

public class RelationToRstreamConverter implements RelationToStreamConverter {
    private TupleDesc td;
    private RelationStreamReader reader;

    public RelationToRstreamConverter(TupleDesc td) {
        this.td = td;
        reader = new RelationStreamReader(td);
    }
    
    public void updateStream(DbIterator nextRelation) throws DbException, TransactionAbortedException {
        reader.updateStream(nextRelation);
    }

    public Stream getStream() {
        return new Stream(reader);
    }
}
