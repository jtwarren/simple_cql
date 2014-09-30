package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbIterator child;
    private TupleDesc td;
    private boolean isIterationOver;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
        isIterationOver = false;
        
        this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        
        isIterationOver = false;
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        isIterationOver = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (isIterationOver)
    		return null;
    	int numDeleted = 0;
        while (child.hasNext()) {
        	Tuple t = child.next();
        	// Works very similarly to insert
        	try {
				Database.getBufferPool().deleteTuple(tid, t);
				numDeleted++;
			} catch (IOException e) {
				throw new DbException("IO error while inserting tuple");
			}
        }
        isIterationOver = true;

        Tuple newTuple = new Tuple(td);
        newTuple.setField(0, new IntField(numDeleted));
        return newTuple;
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	child = children[0];
    }

}
