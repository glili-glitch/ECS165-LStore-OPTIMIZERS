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

            # Transaction already holds this lock
            if transaction_id in lock['holders']:
                # already has X, or is asking again for S
                if lock['type'] == 'X' or lock_type == 'S':
                    return True

                # upgrade S -> X only if this transaction is the sole holder
                if lock['type'] == 'S' and lock_type == 'X':
                    if len(lock['holders']) == 1:
                        lock['type'] = 'X'
                        return True
                    return False

            # compatible shared lock
            if lock['type'] == 'S' and lock_type == 'S':
                lock['holders'].add(transaction_id)
                return True

            # all other cases conflict under no-wait
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