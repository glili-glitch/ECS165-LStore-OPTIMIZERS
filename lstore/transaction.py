import uuid
from lstore import table
from lstore import page


class Transaction:
    """
    Creates a transaction object.
    """

    def __init__(self, db=None):
        self.db = db
        self.queries = []
        self.transaction_id = uuid.uuid4()
        self.held_locks = set()   # set of (table_obj, rid)
        self.rollback_log = []    # entries recorded by queries

    
    
    def add_query(self, query, table_obj, *args):
        self.queries.append((query, table_obj, args))

    """
    Executes all queries in order.
    Returns True if committed, False if aborted.
    """
    def run(self):
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
        Roll back logged changes in reverse order, then release locks.
        """
        for entry in reversed(self.rollback_log):
            action = entry[0]

            # Undo update metadata on base record
            if action == "update":
                _, table_obj, base_rid, old_indirection, old_schema = entry

                with table_obj.directory_lock:
                    if base_rid not in table_obj.page_directory:
                        continue
                    base_entry = table_obj.page_directory[base_rid]

                base_page_range_number = base_entry.page_range_number
                base_page_range = table_obj.page_range_directory[base_page_range_number]
                base_data_locations = base_entry.data_locations

                # Restore indirection value in base page
                ind_loc = base_data_locations[table.INDIRECTION_COLUMN]
                if ind_loc is not None:
                    ind_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][ind_loc.page_number]
                    ind_page.write(old_indirection, ind_loc.offset)

                # Restore schema encoding value in base page
                schema_loc = base_data_locations[table.SCHEMA_ENCODING_COLUMN]
                if schema_loc is not None:
                    schema_page = base_page_range.base_pages[table.SCHEMA_ENCODING_COLUMN][schema_loc.page_number]
                    schema_page.write(old_schema, schema_loc.offset)

            # Undo inserted base record
            elif action == "insert":
                _, table_obj, rid, primary_key, columns = entry

                with table_obj.directory_lock:
                    page_entry = table_obj.page_directory.pop(rid, None)

                if page_entry is not None:
                    page_range = table_obj.page_range_directory[page_entry.page_range_number]
                    if rid in page_range.base_records:
                        del page_range.base_records[rid]

                for i, value in enumerate(columns):
                    table_obj.index.remove_from_index(i, value, rid)

            # Undo index change from update
            elif action == "index_update":
                _, table_obj, base_rid, col_idx, old_val, new_val = entry
                if new_val is not None:
                    table_obj.index.remove_from_index(col_idx, new_val, base_rid)
                if old_val is not None:
                    table_obj.index.add_to_index(col_idx, old_val, base_rid)

        self._release_all_locks()
        self.rollback_log.clear()
        return False

    def commit(self):
        self._release_all_locks()
        self.rollback_log.clear()
        return True

    def _release_all_locks(self):
        # group locks by table so release is cleaner
        locks_by_table = {}
        for table_obj, rid in self.held_locks:
            locks_by_table.setdefault(table_obj, []).append(rid)

        for table_obj, rid_list in locks_by_table.items():
            if table_obj.lock_manager is not None:
                table_obj.lock_manager.release_locks(self.transaction_id, rid_list)

        self.held_locks.clear()