from lstore import table, page
from lstore.table import Record


class Query:
    def __init__(self, table):
        self.table = table

    def insert(self, *columns, transaction=None):
        t = self.table

        if len(columns) != t.num_columns:
            return False

        primary_key = columns[t.key]

        with t.insert_lock:
            existing = t.index.locate(t.key, primary_key)
            if existing:
                return False

            rid = t.allocate_base_rid()
            page_range_number = (rid - 1) // table.NUM_RECORDS_PER_RANGE

            if page_range_number not in t.page_range_directory:
                t.add_page_range(page_range_number)

            schema_encoding = 0
            col_list = list(columns)
            all_columns = [rid, rid, schema_encoding] + col_list

            if transaction and t.lock_manager is not None:
                if not t.lock_manager.acquire_lock(rid, transaction.transaction_id, 'X'):
                    return False
                transaction.held_locks[(t, rid)] = 'X'

            t.add_record(
                page_range_number,
                True,
                *all_columns,
                record=Record(rid, primary_key, col_list)
            )

            for i, value in enumerate(columns):
                t.index.add_to_index(i, value, rid)

            if transaction:
                transaction.rollback_log.append(("insert", t, rid, col_list))

        return True

    def select(self, search_key, search_key_index, projected_columns_index, transaction=None):
        t = self.table
        records = []

        if search_key_index < 0 or search_key_index >= t.num_columns:
            return []

        rid_list = t.index.locate(search_key_index, search_key)
        if not rid_list:
            return []

        proj = list(projected_columns_index[:t.num_columns])
        if len(proj) < t.num_columns:
            proj += [0] * (t.num_columns - len(proj))

        for rid in rid_list:
            entry = t.page_directory.get(rid)
            if entry is None or not entry.is_base or entry.is_deleted:
                continue

            if transaction and t.lock_manager is not None:
                if not t.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks[(t, rid)] = 'S'

            columns = t.construct_full_record(rid, 0)
            if columns is None or len(columns) != t.num_columns:
                continue

            if columns[search_key_index] != search_key:
                continue

            primary_key = t.get_primary_key(rid)

            res_cols = [
                columns[i] if proj[i] == 1 else None
                for i in range(t.num_columns)
            ]

            records.append(Record(rid, primary_key, res_cols))

        return records

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction=None):
        t = self.table
        records = []

        if search_key_index < 0 or search_key_index >= t.num_columns:
            return []

        proj = list(projected_columns_index[:t.num_columns])
        if len(proj) < t.num_columns:
            proj += [0] * (t.num_columns - len(proj))

        with t.directory_lock:
            candidate_rids = [
                rid for rid, entry in t.page_directory.items()
                if entry.is_base and not entry.is_deleted
            ]

        for rid in candidate_rids:
            if transaction and t.lock_manager is not None:
                if not t.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks[(t, rid)] = 'S'

            columns = t.construct_full_record(rid, relative_version)
            if columns is None or len(columns) != t.num_columns:
                continue

            if columns[search_key_index] != search_key:
                continue

            primary_key = columns[t.key]

            res_cols = [
                columns[i] if proj[i] == 1 else None
                for i in range(t.num_columns)
            ]

            records.append(Record(rid, primary_key, res_cols))

        return records

    def delete(self, primary_key, transaction=None):
        t = self.table
        rids = t.index.locate(t.key, primary_key)
        if not rids:
            return False

        base_rid = rids[0]
        base_entry = t.page_directory.get(base_rid)
        if base_entry is None or not base_entry.is_base or base_entry.is_deleted:
            return False

        if transaction and t.lock_manager is not None:
            if not t.lock_manager.acquire_lock(base_rid, transaction.transaction_id, 'X'):
                return False
            transaction.held_locks[(t, base_rid)] = 'X'

        current_values = t.construct_full_record(base_rid, 0)
        if current_values is None or len(current_values) != t.num_columns:
            return False

        if transaction:
            transaction.rollback_log.append(("delete", t, base_rid, list(current_values), base_entry))

        for i, value in enumerate(current_values):
            if value is not None:
                t.index.remove_from_index(i, value, base_rid)

        base_entry.is_deleted = True
        return True

    def update(self, primary_key, *columns, transaction=None):
        t = self.table
        rids = t.index.locate(t.key, primary_key)

        if not rids:
            return False

        if len(columns) != t.num_columns:
            return False

        if columns[t.key] is not None:
            return False

        if not any(val is not None for val in columns):
            return True

        base_rid = rids[0]

        if transaction and t.lock_manager is not None:
            if not t.lock_manager.acquire_lock(base_rid, transaction.transaction_id, 'X'):
                return False
            transaction.held_locks[(t, base_rid)] = 'X'

        current_values = t.construct_full_record(base_rid, 0)
        if current_values is None or len(current_values) != t.num_columns:
            return False

        base_entry = t.page_directory.get(base_rid)
        if base_entry is None or not base_entry.is_base or base_entry.is_deleted:
            return False

        p_range = t.page_range_directory.get(base_entry.page_range_number)
        if p_range is None:
            return False

        locs = base_entry.data_locations
        if locs is None or len(locs) < t.num_columns + 3:
            return False

        ind_col = table.INDIRECTION_COLUMN
        sch_col = table.SCHEMA_ENCODING_COLUMN
        ent_size = page.COLUMN_ENTRY_SIZE

        ind_loc = locs[ind_col]
        sch_loc = locs[sch_col]
        if ind_loc is None or sch_loc is None:
            return False

        ind_page = p_range.base_pages[ind_col][ind_loc.page_number]
        sch_page = p_range.base_pages[sch_col][sch_loc.page_number]

        old_ind = ind_page.read(ind_loc.offset // ent_size)
        old_sch = sch_page.read(sch_loc.offset // ent_size)

        if old_ind is None:
            old_ind = base_rid
        if old_sch is None:
            old_sch = 0

        if transaction:
            transaction.rollback_log.append(("update", t, base_rid, old_ind, old_sch))

        n_cols = t.num_columns
        sch_int = 0
        new_base_sch = old_sch

        for i, val in enumerate(columns):
            if val is not None:
                mask = 1 << (n_cols - 1 - i)
                sch_int |= mask
                new_base_sch |= mask

        copy_tail = None

        if old_sch == 0:
            copy_cols = list(current_values)
            copy_tail = Record(t.allocate_tail_rid(), primary_key, copy_cols)

            t.add_record(
                base_entry.page_range_number,
                False,
                *([copy_tail.rid, base_rid, (1 << n_cols) - 1] + copy_cols),
                record=copy_tail
            )

            if transaction:
                transaction.rollback_log.append(("tail_insert", t, copy_tail.rid))

        prev_rid = base_rid if copy_tail else old_ind
        tail_cols = list(columns)
        tail_rec = Record(t.allocate_tail_rid(), primary_key, tail_cols)

        t.add_record(
            base_entry.page_range_number,
            False,
            *([tail_rec.rid, prev_rid, sch_int] + tail_cols),
            record=tail_rec
        )

        if transaction:
            transaction.rollback_log.append(("tail_insert", t, tail_rec.rid))

        ind_page.write(tail_rec.rid, ind_loc.offset)
        sch_page.write(new_base_sch, sch_loc.offset)

        for i, new_val in enumerate(columns):
            if new_val is not None:
                old_val = current_values[i]
                if old_val != new_val:
                    if transaction:
                        transaction.rollback_log.append(("index_update", t, base_rid, i, old_val, new_val))
                    if old_val is not None:
                        t.index.remove_from_index(i, old_val, base_rid)
                    t.index.add_to_index(i, new_val, base_rid)

        t.trigger_merge_check()
        return True

    def sum(self, start_range, end_range, aggregate_column_index, transaction=None):
        t = self.table
        total = 0
        found = False

        for key in range(start_range, end_range + 1):
            rids = t.index.locate(t.key, key)
            if not rids:
                continue

            rid = rids[0]
            entry = t.page_directory.get(rid)
            if entry is None or not entry.is_base or entry.is_deleted:
                continue

            if transaction and t.lock_manager is not None:
                if not t.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks[(t, rid)] = 'S'

            value = t.get_column_value(rid, aggregate_column_index, 0)
            if value is not None:
                total += value
                found = True

        return total if found else 0

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, transaction=None):
        t = self.table
        total = 0
        found = False

        for key in range(start_range, end_range + 1):
            rids = t.index.locate(t.key, key)
            if not rids:
                continue

            rid = rids[0]
            entry = t.page_directory.get(rid)
            if entry is None or not entry.is_base or entry.is_deleted:
                continue

            if transaction and t.lock_manager is not None:
                if not t.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks[(t, rid)] = 'S'

            value = t.get_column_value(rid, aggregate_column_index, relative_version)
            if value is not None:
                total += value
                found = True

        return total if found else 0

    def increment(self, key, column, transaction=None):
        res = self.select(key, self.table.key, [1] * self.table.num_columns, transaction=transaction)
        if not res:
            return False

        updated = [None] * self.table.num_columns
        updated[column] = res[0].columns[column] + 1
        return self.update(key, *updated, transaction=transaction)
