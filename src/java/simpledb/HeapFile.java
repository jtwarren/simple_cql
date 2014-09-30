package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private File f;
	private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageNumber = pid.pageNumber();
        int size = BufferPool.getPageSize();
        int offset = size * pageNumber;

        RandomAccessFile raf;
        byte[] bytes = new byte[size];
		try {
			raf = new RandomAccessFile(f, "r");
			raf.seek(offset);
			raf.read(bytes);
			raf.close();
			return new HeapPage((HeapPageId) pid, bytes);
		} catch (Exception e) {
			throw new IllegalArgumentException();
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	int pageNumber = page.getId().pageNumber();
        int size = BufferPool.getPageSize();
        int offset = size * pageNumber;

        RandomAccessFile raf;
        byte[] bytes = page.getPageData();
        raf = new RandomAccessFile(f, "rw");
		raf.seek(offset);
		raf.write(bytes);
		raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil((double) f.length() / (double) BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	ArrayList<Page> modifiedPages = new ArrayList<Page>();
    	// First go through all existing pages, to see if there's a vacancy for the
    	// tuple to be inserted in
        for (int i = 0; i < numPages(); i++) {
        	HeapPageId pid = new HeapPageId(getId(), i);
        	HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        	if (page.getNumEmptySlots() > 0) {
        		page.insertTuple(t);
        		modifiedPages.add(page);
        		return modifiedPages;
        	}
        }
        // No space available, so create new empty page
        byte[] emptyPage = HeapPage.createEmptyPageData();
        HeapPageId pid = new HeapPageId(getId(), numPages());
        HeapPage page = new HeapPage(pid, emptyPage);
        page.insertTuple(t);
        
        // Once the new page has been created, flush the page to disk
        writePage(page);
        modifiedPages.add(page);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> modifiedPages = new ArrayList<Page>();
        modifiedPages.add(page);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}

