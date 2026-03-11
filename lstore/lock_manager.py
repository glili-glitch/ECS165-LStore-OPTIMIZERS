import threading


class LockManager:
    def __init__(self):
        # (table_id, rid) -> {'type': 'S' or 'X', 'holders': set(transaction_ids)}
        self.locks = {}
        self.lock_mutex = threading.Lock()

    def acquire_lock(self, table_obj, rid, transaction_id, lock_type):
        """
        No-Wait 2PL:
        - grant immediately if compatible
        - otherwise return False immediately
        """
        lock_key = (id(table_obj), rid)

        with self.lock_mutex:
            if lock_key not in self.locks:
                self.locks[lock_key] = {
                    'type': lock_type,
                    'holders': {transaction_id}
                }
                return True

            lock = self.locks[lock_key]

            # transaction already holds a lock
            if transaction_id in lock['holders']:
                # already has X, or is requesting S while already holding S/X
                if lock['type'] == 'X' or lock_type == 'S':
                    return True

                # upgrade S -> X only if this txn is sole holder
                if lock['type'] == 'S' and lock_type == 'X':
                    if len(lock['holders']) == 1:
                        lock['type'] = 'X'
                        return True
                    return False

            # another txn holds X, or requester wants X while others hold S
            if lock['type'] == 'X':
                return False

            if lock['type'] == 'S':
                if lock_type == 'S':
                    lock['holders'].add(transaction_id)
                    return True
                return False

            return False

    def release_locks(self, table_obj, transaction_id, rids):
        """
        Release all locks held by transaction_id on the given RIDs for one table.
        """
        with self.lock_mutex:
            for rid in rids:
                lock_key = (id(table_obj), rid)

                if lock_key not in self.locks:
                    continue

                lock = self.locks[lock_key]

                if transaction_id in lock['holders']:
                    lock['holders'].remove(transaction_id)

                    if not lock['holders']:
                        del self.locks[lock_key]