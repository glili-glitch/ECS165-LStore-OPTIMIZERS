from itertools import count
from lstore import table, page
from lstore.table import Record

_rid_counter = count(1)

class Query:
    def __init__(self, table):
        self.table = table

    def insert(self, *columns, transaction=None):
        target_table = self.table
        if len(columns) != target_table.num_columns:
            return False

        existing = target_table.index.locate(target_table.key, columns[target_table.key])
        if existing:
            return False

        primary_key = columns[target_table.key]
        page_range_number = primary_key // table.NUM_RECORDS_PER_RANGE

        if page_range_number not in target_table.page_range_directory:
            target_table.add_page_range(page_range_number)

        rid = next(_rid_counter)

        if transaction:
            if not target_table.lock_manager.acquire_lock(rid, transaction.transaction_id, 'X'):
                return False
            transaction.held_locks.add((target_table, rid))

        schema_encoding = 0
        col_list = list(columns)
        all_columns = [rid, rid, schema_encoding] + col_list
        target_table.add_record(page_range_number, True, *all_columns, record=Record(rid, primary_key, col_list))

        for i, value in enumerate(columns):
            target_table.index.add_to_index(i, value, rid)

        if transaction:
            transaction.rollback_log.append(("insert", target_table, rid, primary_key, col_list))

        return True

    def select(self, search_key, search_key_index, projected_columns_index, transaction=None):
        target_table = self.table
        rid_list = target_table.index.locate(search_key_index, search_key)

        if not rid_list:
            rid_list = []
            with target_table.directory_lock:
                for rid, entry in target_table.page_directory.items():
                    if entry.is_base and target_table.get_column_value(rid, search_key_index, 0) == search_key:
                        rid_list.append(rid)

        records = []
        directory = target_table.page_directory
        for rid in rid_list:
            if rid not in directory:
                continue

            if transaction:
                if not target_table.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks.add((target_table, rid))

            columns = target_table.construct_full_record(rid, 0)
            primary_key = target_table.get_primary_key(rid)
            res_cols = [columns[i] for i, p in enumerate(projected_columns_index) if p == 1]
            records.append(Record(rid, primary_key, res_cols))

        return records

    def select_version(self, search_key, search_key_index, proj_index, relative_version, transaction=None):
        target_table = self.table
        rid_list = target_table.index.locate(search_key_index, search_key)

        if not rid_list:
            rid_list = []
            with target_table.directory_lock:
                for rid, entry in target_table.page_directory.items():
                    if entry.is_base and target_table.get_column_value(rid, search_key_index, relative_version) == search_key:
                        rid_list.append(rid)

        records = []
        directory = target_table.page_directory
        for rid in rid_list:
            if rid not in directory:
                continue

            if transaction:
                if not target_table.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks.add((target_table, rid))

            columns = target_table.construct_full_record(rid, relative_version)
            primary_key = target_table.get_primary_key(rid)
            res_cols = [columns[i] for i, p in enumerate(proj_index) if p == 1]
            records.append(Record(rid, primary_key, res_cols))

        return records

    def delete(self, primary_key, transaction=None):
        target_table = self.table
        rids = target_table.index.locate(target_table.key, primary_key)
        if not rids: return False

        base_rid = rids[0]
        if transaction:
            if not target_table.lock_manager.acquire_lock(base_rid, transaction.transaction_id, 'X'):
                return False
            transaction.held_locks.add((target_table, base_rid))

        self.update(primary_key, *([None] * target_table.num_columns), transaction=transaction)

        with target_table.directory_lock:
            target_table.page_directory.pop(base_rid, None)

        return True

    def update(self, primary_key, *columns, transaction=None):
        t = self.table
        rids = t.index.locate(t.key, primary_key)
        if not rids or len(columns) != t.num_columns or columns[t.key] is not None:
            return False

        base_rid = rids[0]
        current_values = t.construct_full_record(base_rid, 0)
        if current_values is None:
           return False
        if transaction:
            if not t.lock_manager.acquire_lock(base_rid, transaction.transaction_id, 'X'):
                return False
            transaction.held_locks.add((t, base_rid))
        if all(v is None for v in columns):
                return True
        base_entry = t.page_directory[base_rid]
        p_range = t.page_range_directory[base_entry.page_range_number]
        locs = base_entry.data_locations

        # Localize frequently used constants/attributes
        ind_col, sch_col, ent_size = table.INDIRECTION_COLUMN, table.SCHEMA_ENCODING_COLUMN, page.COLUMN_ENTRY_SIZE
        
        ind_loc = locs[ind_col]
        ind_page = p_range.base_pages[ind_col][ind_loc.page_number]
        old_ind = ind_page.read(ind_loc.offset // ent_size)

        sch_loc = locs[sch_col]
        sch_page = p_range.base_pages[sch_col][sch_loc.page_number]
        old_sch = sch_page.read(sch_loc.offset // ent_size)

        if transaction:
            transaction.rollback_log.append(("update", t, base_rid, old_ind, old_sch))

        sch_int, new_base_sch, n_cols = 0, old_sch, t.num_columns
        for i, val in enumerate(columns):
            if val is not None:
                mask = 1 << (n_cols - 1 - i)
                sch_int |= mask
                new_base_sch |= mask

        copy_tail = None
        if not old_sch and any(v is not None for v in columns):
            # Optimized list comprehension for base value copying
            copy_cols = [p_range.base_pages[i+3][locs[i+3].page_number].read(locs[i+3].offset // ent_size) for i in range(n_cols)]
            copy_tail = Record(next(_rid_counter), primary_key, copy_cols)
            t.add_record(base_entry.page_range_number, False, *([copy_tail.rid, base_rid, (1 << n_cols) - 1] + copy_cols), record=copy_tail)

        prev_rid = copy_tail.rid if copy_tail else old_ind
        tail_rec = Record(next(_rid_counter), primary_key, list(columns))
        t.add_record(base_entry.page_range_number, False, *([tail_rec.rid, prev_rid, sch_int] + list(columns)), record=tail_rec)

        ind_page.write(tail_rec.rid, ind_loc.offset)
        sch_page.write(new_base_sch, sch_loc.offset)

        with t.directory_lock:
            locs[ind_col] = table.PageCoord(ind_loc.page_number, ind_loc.offset)
            locs[sch_col] = table.PageCoord(sch_loc.page_number, sch_loc.offset)

        # INDEX UPDATE LOGIC: Use Version 1 to find the real old value
        for i, new_val in enumerate(columns):
            if new_val is not None:
                old_val = current_values[i] 
                if old_val is not None and new_val != old_val:
                    if transaction: transaction.rollback_log.append(("index_update", t, base_rid, i, old_val, new_val))
                    t.index.remove_from_index(i, old_val, base_rid)
                    t.index.add_to_index(i, new_val, base_rid)

        t.trigger_merge_check()
        return True

    def sum(self, start_range, end_range, aggregate_column_index, transaction=None):
        t, total, found = self.table, 0, False
        for key in range(start_range, end_range + 1):
            rids = t.index.locate(t.key, key)
            if not rids or rids[0] not in t.page_directory: continue
            rid = rids[0]
            if transaction:
                if not t.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'): return False
                transaction.held_locks.add((t, rid))
            found = True
            total += (t.get_column_value(rid, aggregate_column_index, 0) or 0)
        return total if found else False

    def sum_version(self, start_range, end_range, agg_idx, rel_ver, transaction=None):
        t, total, found = self.table, 0, False
        for key in range(start_range, end_range + 1):
            rids = t.index.locate(t.key, key)
            if not rids or rids[0] not in t.page_directory: continue
            rid = rids[0]
            if transaction:
                if not t.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'): return False
                transaction.held_locks.add((t, rid))
            found = True
            total += (t.get_column_value(rid, agg_idx, rel_ver) or 0)
        return total if found else False

    def increment(self, key, column, transaction=None):
        res = self.select(key, self.table.key, [1] * self.table.num_columns, transaction=transaction)
        if res:
            updated = [None] * self.table.num_columns
            updated[column] = res[0].columns[column] + 1
            return self.update(key, *updated, transaction=transaction)
        return False