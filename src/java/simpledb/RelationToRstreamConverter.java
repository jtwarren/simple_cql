package simpledb;

public class RelationToRstreamConverter implements RelationToStreamConverter {
    private RelationStreamReader reader;

    public RelationToRstreamConverter(TupleDesc td) {
        reader = new RelationStreamReader(td);
    }
    
    public void updateStream(DbIterator nextRelation) throws DbException, TransactionAbortedException {
        reader.updateStream(nextRelation);
    }

    public Stream getStream() {
        return new Stream(reader);
    }
}
