package simpledb;

public interface RelationToStreamConverter {

	public void updateStream(DbIterator nextRelation)
			throws DbException, TransactionAbortedException;
	
	public Stream getStream();
}
