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
				// Check if exclusive locks are held
				if (exclusiveLocks.get(pid) == null || exclusiveLocks.get(pid) == tid) {
					// Acquire shared lock
					sharedLocks.putIfAbsent(pid, new ArrayList<TransactionId>());
					sharedLocks.get(pid).add(tid);
					break;
				}
				
				// Timeout if necessary
				if ((System.currentTimeMillis() - ts) > TIMEOUT) {
					throw new TransactionAbortedException();
				}
			}
		} else if (perm == Permissions.READ_WRITE) {
			// Return if we already have write permissions.
			if (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid) == tid) {
				return;
			}
			
			// Attempt to acquire write exclusive lock
			while (true) {
				
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
				
				// Timeout if necessary
				if ((System.currentTimeMillis() - ts) > TIMEOUT) {
					throw new TransactionAbortedException();
				}
			}
		}
	}

	public void release(TransactionId tid, PageId pid) {
		exclusiveLocks.remove(pid);
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
