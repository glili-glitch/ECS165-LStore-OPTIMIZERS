import threading

class LockManager:
    def __init__(self):
        self.locks = {}  # RID -> {'type': 'S'/'X', 'holders': set(transaction_ids)
        self.lock_mutex = threading.Lock()

    def acquire_lock(self, rid, transaction_id, lock_type):
        """
        Acquire a lock on a record (RID) for a transaction.
        Implements No-Wait: Returns False if lock cannot be granted immediately.
        """
        with self.lock_mutex:
            if rid not in self.locks:
                self.locks[rid] = {'type': lock_type, 'holders': {transaction_id}}
                return True
            
            lock = self.locks[rid]
            
            # Case 1: Transaction already holds the lock
            if transaction_id in lock['holders']:
                if lock['type'] == 'X' or lock_type == 'S':
                    return True
                # Upgrade S -> X
                if len(lock['holders']) == 1:
                    lock['type'] = 'X'
                    return True
                else:
                    # Others hold S-lock, cannot upgrade
                    return False

            # Case 2: No-Wait logic for new holders
            if lock['type'] == 'S' and lock_type == 'S':
                lock['holders'].add(transaction_id)
                return True
            
            # Conflict (X-lock exists, or trying to acquire X-lock while S-locks exist)
            return False

    def release_locks(self, transaction_id, rids):
        """Release all locks held by a specific transaction."""
        with self.lock_mutex:
            
            for item in rids:
                rid = item[1] if isinstance(item, tuple) else item

                if rid in self.locks:
                    lock = self.locks[rid]
                    if transaction_id in lock['holders']:
                        lock['holders'].remove(transaction_id)
                        if not lock['holders']:
                            del self.locks[rid]
