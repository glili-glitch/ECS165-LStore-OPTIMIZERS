import threading


class LockManager:
    def __init__(self):
        # (table_id, rid) -> {'readers': set(txn_ids), 'writer': txn_id or None}
        self.locks = {}
        self.mutex = threading.Lock()

    def acquire_lock(self, table_obj, rid, transaction_id, lock_type):
        key = (id(table_obj), rid)

        with self.mutex:
            if key not in self.locks:
                self.locks[key] = {
                    'readers': set(),
                    'writer': None,
                }

            lock = self.locks[key]
            readers = lock['readers']
            writer = lock['writer']

            if lock_type == 'S':
                if writer == transaction_id:
                    return True
                if transaction_id in readers:
                    return True
                if writer is None:
                    readers.add(transaction_id)
                    return True
                return False

            if lock_type == 'X':
                if writer == transaction_id:
                    return True

                # Upgrade S -> X only if this transaction is the only reader.
                if transaction_id in readers:
                    if len(readers) == 1 and writer is None:
                        readers.remove(transaction_id)
                        lock['writer'] = transaction_id
                        return True
                    return False

                # Fresh X request.
                if writer is None and len(readers) == 0:
                    lock['writer'] = transaction_id
                    return True
                return False

            return False

    def release_locks(self, table_obj, transaction_id, rids):
        with self.mutex:
            for rid in rids:
                key = (id(table_obj), rid)
                lock = self.locks.get(key)
                if lock is None:
                    continue

                if lock['writer'] == transaction_id:
                    lock['writer'] = None

                if transaction_id in lock['readers']:
                    lock['readers'].remove(transaction_id)

                if lock['writer'] is None and len(lock['readers']) == 0:
                    del self.locks[key]
