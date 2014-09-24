package simpledb;

import java.util.List;
import java.util.NoSuchElementException;

public class AggregateIterator implements DbIterator {

	private static final long serialVersionUID = 1L;

	private List<Tuple> tuples;
	private TupleDesc td;
	
	private int curIndex;
	private boolean open;
	
	public AggregateIterator(List<Tuple> tuples, TupleDesc td) {
		this.tuples = tuples;
		this.td = td;
		
		open = false;
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		curIndex = 0;
		open = true;
	}
		      
	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if (!open) {
			throw new IllegalStateException();
		}
		if (curIndex < tuples.size())
			return true;
		return false;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException {
		if (!open) {
			throw new IllegalStateException();
		}
		if (curIndex >= tuples.size()) {
			throw new NoSuchElementException();
		}
		Tuple tuple = tuples.get(curIndex);
		curIndex++;
		return tuple;
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		if (!open) {
			throw new IllegalStateException();
		}
		curIndex = 0;
	}

	@Override
	public TupleDesc getTupleDesc() {
		return td;
	}

	@Override
	public void close() {
		open = false;
	}

}
