package simpledb;

public class RelationStreamConverter extends Operator {
	
	private DbIterator relation;
	
	public RelationStreamConverter(DbIterator relation) {
		this.relation = relation;
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
