import uuid

class Transaction:
    """
    Creates a transaction object.
    Ensures ACID properties by holding all locks until commit/abort (Strict 2PL).
    """

    def __init__(self):
        self.queries = []
        self.transaction_id = uuid.uuid4()
        # Maps (table_obj, rid) -> lock_type ('S' for Shared, 'X' for Exclusive)
        self.held_locks = {}  
        # Stores (action, table, rid, old_value, old_schema)
        self.rollback_log = []

    def add_query(self, query, table_obj, *args):
        """
        Adds a query to the transaction's execution list.
        """
        self.queries.append((query, table_obj, args))

    def run(self):
        """
        Executes queries sequentially.
        If any query fails (e.g., lock timeout or constraint violation), aborts.
        """
        try:
            for query, table_obj, args in self.queries:
                # The query MUST take 'transaction=self' as a keyword argument
                result = query(*args, transaction=self)
                if result is False:
                    return self.abort()
            return self.commit()
        except Exception as e:
            # Catching unexpected errors to ensure we always release locks
            print(f"Transaction execution error: {e}")
            return self.abort()

    def abort(self):
        """
        Undo changes in REVERSE order to ensure data consistency.
        Uses Table-level rollback to avoid physical page corruption.
        """
        for entry in reversed(self.rollback_log):
            action = entry[0]

            if action == "update":
                # entry: ("update", table_obj, rid, old_indirection, old_schema)
                _, table_obj, rid, old_indirection, old_schema = entry
                table_obj.rollback_record(rid, old_indirection, old_schema)

            elif action == "insert":
                # entry: ("insert", table_obj, rid, [column_values])
                _, table_obj, rid, values = entry
                table_obj.delete_record(rid, values)

            elif action == "index_update":
                # entry: ("index_update", table_obj, rid, col_idx, old_val, new_val)
                _, table_obj, rid, col_idx, old_val, new_val = entry
                if new_val is not None:
                    table_obj.index.remove_from_index(col_idx, new_val, rid)
                if old_val is not None:
                    table_obj.index.add_to_index(col_idx, old_val, rid)

        self.rollback_log.clear()
        self._release_all_locks()
        return False

    def commit(self):
        """
        Finalize the transaction by clearing the log and releasing all locks.
        """
        self.rollback_log.clear()
        self._release_all_locks()
        return True

    def _release_all_locks(self):
        """
        Releases every lock acquired during the transaction life cycle.
        """
        # Group RIDs by table to minimize lock manager calls
        lock_groups = {}
        for (table_obj, rid) in self.held_locks.keys():
            if table_obj not in lock_groups:
                lock_groups[table_obj] = []
            lock_groups[table_obj].append(rid)

        for table_obj, rids in lock_groups.items():
            # Release all locks for this transaction on this specific table
            table_obj.lock_manager.release_locks(self.transaction_id, rids)
        
        self.held_locks.clear()