import uuid

class Transaction:
    def __init__(self):
        self.queries = []
        self.transaction_id = uuid.uuid4()
        self.held_locks = {}  # (table_obj, rid) -> 'S' or 'X'
        self.rollback_log = []

    def add_query(self, query, table_obj, *args):
        self.queries.append((query, table_obj, args))

    def run(self):
        try:
            for query, table_obj, args in self.queries:
                result = query(*args, transaction=self)
                if result is False:
                    return self.abort()
            return self.commit()
        except Exception as e:
            print("Transaction error:", e)
            return self.abort()

    def abort(self):
        # Reverse order is vital for multiple updates on one record
        for entry in reversed(self.rollback_log):
            action = entry[0]
            if action == "update":
                _, table_obj, rid, old_ind, old_sch = entry
                table_obj.rollback_record(rid, old_ind, old_sch)
            elif action == "insert":
                _, table_obj, rid, columns = entry
                table_obj.delete_record(rid, columns)
            elif action == "index_update":
                _, table_obj, rid, col_idx, old_val, new_val = entry
                if new_val is not None:
                    table_obj.index.remove_from_index(col_idx, new_val, rid)
                if old_val is not None:
                    table_obj.index.add_to_index(col_idx, old_val, rid)

        self.rollback_log.clear()
        self._release_all_locks()
        return False

    def commit(self):
        self.rollback_log.clear()
        self._release_all_locks()
        return True

    def _release_all_locks(self):
        # Group by table for efficiency
        groups = {}
        for (table_obj, rid) in self.held_locks.keys():
            groups.setdefault(table_obj, []).append(rid)
        for table_obj, rids in groups.items():
            table_obj.lock_manager.release_locks(self.transaction_id, rids)
        self.held_locks.clear()