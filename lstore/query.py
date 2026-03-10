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

        # Make duplicate-check + RID allocation + insert atomic
        with t.insert_lock:
            existing = t.index.locate(t.key, primary_key)
            if existing:
                return False

            rid = t.allocate_rid()
            page_range_number = (rid - 1) // table.NUM_RECORDS_PER_RANGE

            if page_range_number not in t.page_range_directory:
                t.add_page_range(page_range_number)

            schema_encoding = 0
            col_list = list(columns)
            all_columns = [rid, rid, schema_encoding] + col_list

            if transaction:
                if not t.lock_manager.acquire_lock(rid, transaction.transaction_id, 'X'):
                    return False
                transaction.held_locks[(t, rid)] = 'X'
                transaction.rollback_log.append(("insert", t, rid, col_list))

            t.add_record(
                page_range_number,
                True,
                *all_columns,
                record=Record(rid, primary_key, col_list)
            )

            for i, value in enumerate(columns):
                t.index.add_to_index(i, value, rid)

        return True

    def select(self, search_key, search_key_index, projected_columns_index, transaction=None):
        t = self.table
        records = []

        rid_list = t.index.locate(search_key_index, search_key)
        if not rid_list:
            rid_list = []
            with t.directory_lock:
                for rid, entry in t.page_directory.items():
                    if entry.is_base:
                        rid_list.append(rid)

        for rid in rid_list:
            if rid not in t.page_directory:
                continue

            actual_value = t.get_column_value(rid, search_key_index, 0)
            if actual_value != search_key:
                continue

            if transaction:
                if not t.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks[(t, rid)] = 'S'

            columns = t.construct_full_record(rid, 0)
            if columns is None:
                continue

            primary_key = t.get_primary_key(rid)

            proj = list(projected_columns_index[:t.num_columns])
            if len(proj) < t.num_columns:
                proj += [0] * (t.num_columns - len(proj))

            res_cols = [
                columns[i] if proj[i] == 1 else None
                for i in range(t.num_columns)
            ]

            records.append(Record(rid, primary_key, res_cols))

        return records

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction=None):
        t = self.table
        records = []

        with t.directory_lock:
            candidate_rids = [
                rid for rid, entry in t.page_directory.items()
                if entry.is_base
            ]

        for rid in candidate_rids:
            if rid not in t.page_directory:
                continue

            value = t.get_column_value(rid, search_key_index, relative_version)
            if value != search_key:
                continue

            if transaction:
                if not t.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks[(t, rid)] = 'S'

            columns = t.construct_full_record(rid, relative_version)
            if columns is None:
                continue

            primary_key = t.get_primary_key(rid)

            proj = list(projected_columns_index[:t.num_columns])
            if len(proj) < t.num_columns:
                proj += [0] * (t.num_columns - len(proj))

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

        if transaction:
            if not t.lock_manager.acquire_lock(base_rid, transaction.transaction_id, 'X'):
                return False
            transaction.held_locks[(t, base_rid)] = 'X'

        current_values = t.construct_full_record(base_rid, 0)
        if current_values is None:
            return False

        for i, value in enumerate(current_values):
            if value is not None:
                t.index.remove_from_index(i, value, base_rid)

        with t.directory_lock:
            t.page_directory.pop(base_rid, None)

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

        if transaction:
            if not t.lock_manager.acquire_lock(base_rid, transaction.transaction_id, 'X'):
                return False
            transaction.held_locks[(t, base_rid)] = 'X'

        current_values = t.construct_full_record(base_rid, 0)
        if current_values is None:
            return False

        base_entry = t.page_directory[base_rid]
        p_range = t.page_range_directory[base_entry.page_range_number]
        locs = base_entry.data_locations

        ind_col = table.INDIRECTION_COLUMN
        sch_col = table.SCHEMA_ENCODING_COLUMN
        ent_size = page.COLUMN_ENTRY_SIZE

        ind_loc = locs[ind_col]
        ind_page = p_range.base_pages[ind_col][ind_loc.page_number]
        old_ind = ind_page.read(ind_loc.offset // ent_size)

        sch_loc = locs[sch_col]
        sch_page = p_range.base_pages[sch_col][sch_loc.page_number]
        old_sch = sch_page.read(sch_loc.offset // ent_size)

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

        if not old_sch:
            copy_cols = [
                p_range.base_pages[i + 3][locs[i + 3].page_number].read(locs[i + 3].offset // ent_size)
                for i in range(n_cols)
            ]
            copy_tail = Record(t.allocate_rid(), primary_key, copy_cols)
            t.add_record(
                base_entry.page_range_number,
                False,
                *([copy_tail.rid, base_rid, (1 << n_cols) - 1] + copy_cols),
                record=copy_tail
            )

        prev_rid = copy_tail.rid if copy_tail else old_ind
        tail_rec = Record(t.allocate_rid(), primary_key, list(columns))

        t.add_record(
            base_entry.page_range_number,
            False,
            *([tail_rec.rid, prev_rid, sch_int] + list(columns)),
            record=tail_rec
        )

        ind_page.write(tail_rec.rid, ind_loc.offset)
        sch_page.write(new_base_sch, sch_loc.offset)

        with t.directory_lock:
            locs[ind_col] = table.PageCoord(ind_loc.page_number, ind_loc.offset)
            locs[sch_col] = table.PageCoord(sch_loc.page_number, sch_loc.offset)

        for i, new_val in enumerate(columns):
            if new_val is not None:
                old_val = current_values[i]
                if old_val is not None and new_val != old_val:
                    if transaction:
                        transaction.rollback_log.append(("index_update", t, base_rid, i, old_val, new_val))
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
            if rid not in t.page_directory:
                continue

            if transaction:
                if not t.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks[(t, rid)] = 'S'

            value = t.get_column_value(rid, aggregate_column_index, 0)
            if value is not None:
                total += value
            found = True

        return total if found else False

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, transaction=None):
        t = self.table
        total = 0
        found = False

        for key in range(start_range, end_range + 1):
            rids = t.index.locate(t.key, key)
            if not rids:
                continue

            rid = rids[0]
            if rid not in t.page_directory:
                continue

            if transaction:
                if not t.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks[(t, rid)] = 'S'

            value = t.get_column_value(rid, aggregate_column_index, relative_version)
            if value is not None:
                total += value
            found = True

        return total if found else False

    def increment(self, key, column, transaction=None):
        res = self.select(key, self.table.key, [1] * self.table.num_columns, transaction=transaction)
        if not res:
            return False

        updated = [None] * self.table.num_columns
        updated[column] = res[0].columns[column] + 1
        return self.update(key, *updated, transaction=transaction)