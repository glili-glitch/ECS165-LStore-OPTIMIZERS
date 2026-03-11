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
                    "readers": set(),
                    "writer": None
                }

            lock = self.locks[key]
            readers = lock["readers"]
            writer = lock["writer"]

            if lock_type == 'S':
                # already holds X or S
                if writer == transaction_id:
                    return True
                if transaction_id in readers:
                    return True

                # no writer -> grant shared
                if writer is None:
                    readers.add(transaction_id)
                    return True

                return False

            elif lock_type == 'X':
                # already holds X
                if writer == transaction_id:
                    return True

                # upgrade S -> X if this txn is the only reader
                if transaction_id in readers:
                    if len(readers) == 1 and writer is None:
                        readers.remove(transaction_id)
                        lock["writer"] = transaction_id
                        return True
                    return False

                # fresh X request: only allowed if no readers and no writer
                if writer is None and len(readers) == 0:
                    lock["writer"] = transaction_id
                    return True

                return False

            return False

    def release_locks(self, table_obj, transaction_id, rids):
        with self.mutex:
            for rid in rids:
                key = (id(table_obj), rid)
                if key not in self.locks:
                    continue

                lock = self.locks[key]

                if lock["writer"] == transaction_id:
                    lock["writer"] = None

                if transaction_id in lock["readers"]:
                    lock["readers"].remove(transaction_id)

                if lock["writer"] is None and len(lock["readers"]) == 0:
                    del self.locks[key]