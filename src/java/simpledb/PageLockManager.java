package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class PageLockManager {
	
	public static final long TIMEOUT = 300;

	private final ConcurrentHashMap<PageId, List<TransactionId>> sharedLocks;
	private final ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;

	public PageLockManager() {
		sharedLocks = new ConcurrentHashMap<PageId, List<TransactionId>>();
		exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
	}

	public void acquire(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
		long ts = System.currentTimeMillis();
		if (perm == Permissions.READ_ONLY) {
			// Return if we already have read permissions.
			if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid)) {
				return;
			}
			// Return if we already have write permissions.
			if (exclusiveLocks.containsKey(pid) && tid.equals(exclusiveLocks.get(pid))) {
				return;
			}
			
			// Attempt to acquire shared lock
			while (true) {
				// Timeout if necessary
				if ((System.currentTimeMillis() - ts) > TIMEOUT) {
					throw new TransactionAbortedException();
				}

				// Check if exclusive locks are held
				if (exclusiveLocks.get(pid) == null || exclusiveLocks.get(pid) == tid) {
					// Acquire shared lock
					sharedLocks.putIfAbsent(pid, new ArrayList<TransactionId>());
					sharedLocks.get(pid).add(tid);
					break;
				}
				
			}
		} else if (perm == Permissions.READ_WRITE) {
			// Return if we already have write permissions.
			if (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid) == tid) {
				return;
			}
			
			// Attempt to acquire write exclusive lock
			while (true) {
				// Timeout if necessary
				if ((System.currentTimeMillis() - ts) > TIMEOUT) {
					throw new TransactionAbortedException();
				}
				
				// Check exclusive locks
				if (exclusiveLocks.get(pid) != null) {
					continue;
				}
				
				// Take exclusive lock if there are no shared locks
				if (sharedLocks.get(pid) == null || sharedLocks.get(pid).isEmpty()) {
					exclusiveLocks.put(pid, tid);
					break;
				}
				
				// Take exclusive lock if we hold the only shared lock
				if (sharedLocks.get(pid).size() == 1 && sharedLocks.get(pid).contains(tid)) {
					exclusiveLocks.put(pid, tid);
					break;
				}

			}
		}
	}

	public void release(TransactionId tid, PageId pid) {
		exclusiveLocks.remove(pid, tid);
		if (sharedLocks.containsKey(pid)) {
			sharedLocks.get(pid).remove(tid);
		}
	}

	public void release(TransactionId tid) {
		// Release shared locks
		for (Entry<PageId, List<TransactionId>> entry : sharedLocks.entrySet()) {
        	List<TransactionId> tids = entry.getValue();
        	tids.remove(tid);
        }
		
		// Release exclusive locks
		for (PageId pid : exclusiveLocks.keySet()) {
			exclusiveLocks.remove(pid, tid);
		}

	}
	
	public List<PageId> getAffectedPages(TransactionId tid) {
		List<PageId> affectedPages = new ArrayList<PageId> ();
		for (Entry<PageId, List<TransactionId>> entry : sharedLocks.entrySet()) {
			if (entry.getValue().contains(tid)) {
				affectedPages.add(entry.getKey());
			}
		}
				
		for (Entry<PageId, TransactionId> entry : exclusiveLocks.entrySet()) {
			if (entry.getValue().equals(tid)) {
				affectedPages.add(entry.getKey());
			}
		}
		return affectedPages;
	}

	public boolean holdsLock(TransactionId tid, PageId pid) {
		// Check if there is a shared lock
		if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid)) {
			return true;
		}
		// Check if there is an exclusive lock
		if (exclusiveLocks.containsKey(pid) && tid.equals(exclusiveLocks.get(pid))) {
			return true;
		}
		
		return false;
	}
}
