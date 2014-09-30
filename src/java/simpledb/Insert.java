package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private TupleDesc td;
    private boolean isIterationOver;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
    	TupleDesc tableDesc = Database.getCatalog().getTupleDesc(tableid);
    	if (!child.getTupleDesc().equals(tableDesc)) {
    		throw new DbException("Schemas don't match!");
    	}
        this.tid = t;
        this.child = child;
        this.tableid = tableid;
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (isIterationOver)
    		return null;
    	int numInserted = 0;
        while (child.hasNext()) {
        	Tuple t = child.next();
        	try {
        		// Insert new tuple into the page (which is accessed through the BufferPool)
				Database.getBufferPool().insertTuple(tid, tableid, t);
				numInserted++;
			} catch (IOException e) {
				throw new DbException("IO error while inserting tuple");
			}
        }
        // Mark iteration as over, so future insertion operations will fail
        isIterationOver = true;
        
        Tuple newTuple = new Tuple(td);
        newTuple.setField(0, new IntField(numInserted));
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
