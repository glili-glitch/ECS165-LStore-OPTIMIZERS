import threading


class LockManager:
    def __init__(self):
        # rid -> {'type': 'S' or 'X', 'holders': set(transaction_ids)}
        self.locks = {}
        self.lock_mutex = threading.Lock()

    def acquire_lock(self, rid, transaction_id, lock_type):
        """
        No-Wait 2PL:
        - grant immediately if compatible
        - otherwise return False immediately
        """
        with self.lock_mutex:
            if rid not in self.locks:
                self.locks[rid] = {
                    'type': lock_type,
                    'holders': {transaction_id}
                }
                return True

            lock = self.locks[rid]

            

        # CASE 1: Transaction already holds a lock [cite: 39, 40]
            if transaction_id in lock['holders']:
                if lock['type'] == 'X' or lock_type == 'S':
                    return True
                # Upgrade S -> X 
                if len(lock['holders']) == 1:
                    lock['type'] = 'X'
                    return True
                return False # No-Wait: others hold S, so abort 

            # CASE 2: New transaction requesting a lock 
            # No-Wait: If record is X-locked, or new request is X while others have S
            if lock['type'] == 'X' or lock_type == 'X':
                return False 
            
            if lock['type'] == 'S' and lock_type == 'S':
                lock['holders'].add(transaction_id)
                return True

            return False

    def release_locks(self, transaction_id, rids):
        """
        Release all locks held by transaction_id on the given RIDs.
        """
        with self.lock_mutex:
            for rid in rids:
                if rid in self.locks:
                    lock = self.locks[rid]

                    if transaction_id in lock['holders']:
                        lock['holders'].remove(transaction_id)

                        if not lock['holders']:
                            del self.locks[rid]
                        elif lock['type'] == 'X':
                            # defensive fallback: X should only have one holder,
                            # but if state ever gets inconsistent, downgrade safely
                            lock['type'] = 'S'