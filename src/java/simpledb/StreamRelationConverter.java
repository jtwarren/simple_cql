package simpledb;

public class StreamRelationConverter extends Operator {
	
	public StreamRelationConverter(Stream stream) {
		
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub

	}

	@Override
	protected Tuple fetchNext() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DbIterator[] getChildren() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setChildren(DbIterator[] children) {
		// TODO Auto-generated method stub

	}

	@Override
	public TupleDesc getTupleDesc() {
		// TODO Auto-generated method stub
		return null;
	}

}
