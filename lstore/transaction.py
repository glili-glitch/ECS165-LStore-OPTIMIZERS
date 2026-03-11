import uuid


class Transaction:
    def __init__(self):
        self.queries = []
        self.transaction_id = uuid.uuid4()
        self.held_locks = {}   # (table_obj, rid) -> 'S' or 'X'
        self.rollback_log = []

    def add_query(self, query, table_obj, *args):
        self.queries.append((query, table_obj, args))

    def run(self):
        self.rollback_log = []
        self.held_locks = {}
        self.transaction_id = uuid.uuid4()

        try:
            for query, table_obj, args in self.queries:
                result = query(*args, transaction=self)
                if result is False:
                    return self.abort()
            return self.commit()
        except Exception:
            return self.abort()

    def abort(self):
        try:
            for entry in reversed(self.rollback_log):
                action = entry[0]

                if action == "index_update":
                    _, table_obj, rid, col_idx, old_val, new_val = entry

                    if new_val is not None:
                        table_obj.index.remove_from_index(col_idx, new_val, rid)
                    if old_val is not None:
                        table_obj.index.add_to_index(col_idx, old_val, rid)

                elif action == "tail_insert":
                    _, table_obj, tail_rid = entry
                    table_obj.delete_tail_record(tail_rid)

                elif action == "update":
                    _, table_obj, rid, old_ind, old_sch = entry
                    table_obj.rollback_record(rid, old_ind, old_sch)

                elif action == "insert":
                    _, table_obj, rid, columns = entry
                    table_obj.delete_record(rid, columns)

                elif action == "delete":
                    _, table_obj, rid, old_values, base_entry, base_record = entry

                    # Restore page directory entry if missing
                    if rid not in table_obj.page_directory:
                        table_obj.page_directory[rid] = base_entry

                    restored_entry = table_obj.page_directory.get(rid)
                    if restored_entry is not None:
                        if hasattr(restored_entry, "is_deleted"):
                            restored_entry.is_deleted = False
                        elif hasattr(restored_entry, "deleted"):
                            restored_entry.deleted = False

                    # Restore base record into page range if needed
                    pr = table_obj.page_range_directory.get(base_entry.page_range_number)
                    if pr is not None and base_record is not None:
                        if rid not in pr.base_records:
                            pr.base_records[rid] = base_record
                            pr.num_records += 1

                    # Restore index entries
                    for i, value in enumerate(old_values):
                        if value is not None:
                            table_obj.index.add_to_index(i, value, rid)

        finally:
            self.rollback_log.clear()
            self._release_all_locks()

        return False

    def commit(self):
        self.rollback_log.clear()
        self._release_all_locks()
        return True

    def _release_all_locks(self):
        groups = {}

        for (table_obj, rid) in self.held_locks.keys():
            groups.setdefault(table_obj, []).append(rid)

        for table_obj, rids in groups.items():
            if table_obj.lock_manager is not None:
                table_obj.lock_manager.release_locks(self.transaction_id, rids)

        self.held_locks.clear()