import uuid
from lstore import table


class Transaction:
    """
    Creates a transaction object.
    """

    def __init__(self, db=None):
        self.db = db
        self.queries = []
        self.transaction_id = uuid.uuid4()
        self.held_locks = set()   # (table_obj, rid)
        self.rollback_log = []

    def add_query(self, query, table_obj, *args):
        self.queries.append((query, table_obj, args))

    def run(self):
        """
        Executes queries sequentially.
        Aborts immediately on first failure.
        """
        try:
            for query, table_obj, args in self.queries:
                result = query(*args, transaction=self)
                if result is False:
                    return self.abort()
            return self.commit()
        except Exception:
            return self.abort()

    def abort(self):
        """
        Undo changes in reverse order, then release all locks.
        """
        for entry in reversed(self.rollback_log):
            action = entry[0]

            if action == "update":
                _, table_obj, base_rid, old_indirection, old_schema = entry

                with table_obj.directory_lock:
                    base_entry = table_obj.page_directory.get(base_rid)

                if base_entry is None:
                    continue

                page_range = table_obj.page_range_directory[base_entry.page_range_number]
                data_locations = base_entry.data_locations

                # Restore base indirection
                ind_loc = data_locations[table.INDIRECTION_COLUMN]
                if ind_loc is not None:
                    ind_page = page_range.base_pages[table.INDIRECTION_COLUMN][ind_loc.page_number]
                    ind_page.write(old_indirection, ind_loc.offset)

                # Restore base schema encoding
                schema_loc = data_locations[table.SCHEMA_ENCODING_COLUMN]
                if schema_loc is not None:
                    schema_page = page_range.base_pages[table.SCHEMA_ENCODING_COLUMN][schema_loc.page_number]
                    schema_page.write(old_schema, schema_loc.offset)

            elif action == "insert":
                _, table_obj, rid, primary_key, columns = entry

                with table_obj.directory_lock:
                    page_entry = table_obj.page_directory.pop(rid, None)

                if page_entry is not None:
                    page_range = table_obj.page_range_directory[page_entry.page_range_number]

                    if hasattr(page_range, "base_records") and rid in page_range.base_records:
                        del page_range.base_records[rid]

                for i, value in enumerate(columns):
                    table_obj.index.remove_from_index(i, value, rid)

            elif action == "index_update":
                _, table_obj, base_rid, col_idx, old_val, new_val = entry

                if new_val is not None:
                    table_obj.index.remove_from_index(col_idx, new_val, base_rid)

                if old_val is not None:
                    table_obj.index.add_to_index(col_idx, old_val, base_rid)

        self.rollback_log.clear()
        self._release_all_locks()
        return False

    def commit(self):
        """
        Commit transaction by releasing all locks.
        """
        self.rollback_log.clear()
        self._release_all_locks()
        return True

    def _release_all_locks(self):
        """
        Release all locks held by this transaction.
        Supports either:
        - release_locks(transaction_id, rid_list)
        - release_lock(rid, transaction_id)
        """
        locks_by_table = {}

        for table_obj, rid in self.held_locks:
            locks_by_table.setdefault(table_obj, []).append(rid)

        for table_obj, rid_list in locks_by_table.items():
            if getattr(table_obj, "lock_manager", None) is None:
                continue

            lm = table_obj.lock_manager

            if hasattr(lm, "release_locks"):
                lm.release_locks(self.transaction_id, rid_list)
            else:
                for rid in rid_list:
                    lm.release_lock(rid, self.transaction_id)

        self.held_locks.clear()