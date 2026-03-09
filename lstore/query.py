from itertools import count

from lstore import table
from lstore import page
from lstore.table import Table, Record
from lstore.index import Index

_rid_counter = count(1)


class Query:
    """
    Creates a Query object that can perform different queries on the specified table.
    Queries that fail must return False.
    Queries that succeed should return the result or True.
    Any query that crashes should return False.
    """

    def __init__(self, table):
        self.table = table

    def insert(self, *columns, transaction=None):
        existing = self.table.index.locate(self.table.key, columns[self.table.key])
        if existing is not None and len(existing) > 0:
            return False

        if len(columns) != self.table.num_columns:
            return False

        primary_key = columns[self.table.key]
        page_range_number = primary_key // table.NUM_RECORDS_PER_RANGE

        if page_range_number not in self.table.page_range_directory:
            self.table.add_page_range(page_range_number)

        rid = next(_rid_counter)

        if transaction:
            if not self.table.lock_manager.acquire_lock(rid, transaction.transaction_id, 'X'):
                return False
            transaction.held_locks.add((self.table, rid))

        schema_encoding = '0' * self.table.num_columns
        record = Record(rid, primary_key, list(columns))
        all_columns = [rid, rid, int(schema_encoding, 2)] + list(columns)

        self.table.add_record(page_range_number, True, *all_columns, record=record)

        for i, column in enumerate(columns):
            self.table.index.add_to_index(i, column, rid)
            if transaction:
                 transaction.rollback_log.append(
            ("insert", self.table, rid, primary_key, list(columns))
        )

        return True

    def select(self, search_key, search_key_index, projected_columns_index, transaction=None):
        rid_list = self.table.index.locate(search_key_index, search_key)

        if not rid_list:
            rid_list = []
            with self.table.directory_lock:
                all_base_rids = [
                    rid for rid, entry in self.table.page_directory.items()
                    if entry.is_base
                ]
            for rid in all_base_rids:
                val = self.table.get_column_value(rid, search_key_index, 0)
                if val == search_key:
                    rid_list.append(rid)

        record_list = []
        for rid in rid_list:
            if rid not in self.table.page_directory:
                continue

            if transaction:
                if not self.table.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks.add((self.table, rid))
                # extra safety: confirm this rid really matches the requested search key
                actual_val = self.table.get_column_value(rid, search_key_index, 0)
                if actual_val != search_key:
               
                    continue

            columns = self.table.construct_full_record(rid, 0)
            primary_key = self.table.get_primary_key(rid)

            new_columns = []
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    new_columns.append(columns[i])

            record_list.append(Record(rid, primary_key, new_columns))

        return record_list

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction=None):
        version = -relative_version
        rid_list = self.table.index.locate(search_key_index, search_key)

        if not rid_list:
            rid_list = []
            with self.table.directory_lock:
                rid_list = [
                    rid for rid, entry in self.table.page_directory.items()
                    if entry.is_base
                ]

        valid_rids = []
        for rid in rid_list:
            if rid not in self.table.page_directory:
                continue
            val = self.table.get_column_value(rid, search_key_index,version)
            if val == search_key:
                valid_rids.append(rid)

        record_list = []
        for rid in valid_rids:
            if rid not in self.table.page_directory:
                continue

            actual_val = self.table.get_column_value(rid, search_key_index,version)
            if actual_val != search_key:
                continue

            if transaction:
                if not self.table.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks.add((self.table, rid))

            columns = self.table.construct_full_record(rid, relative_version)
            primary_key = self.table.get_primary_key(rid)

            new_columns = []
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    new_columns.append(columns[i])

            record_list.append(Record(rid, primary_key, new_columns))

        return record_list

    def delete(self, primary_key, transaction=None):
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids:
            return False

        base_rid = rids[0]

        if transaction:
            if not self.table.lock_manager.acquire_lock(base_rid, transaction.transaction_id, 'X'):
                return False
            transaction.held_locks.add((self.table, base_rid))

        self.update(primary_key, *([None] * self.table.num_columns), transaction=transaction)

        with self.table.directory_lock:
            if base_rid in self.table.page_directory:
                del self.table.page_directory[base_rid]

        return True

    def update(self, primary_key, *columns, transaction=None):
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids:
            return False

        base_rid = rids[0]

        if columns[self.table.key] is not None:
            return False

        if transaction:
            if not self.table.lock_manager.acquire_lock(base_rid, transaction.transaction_id, 'X'):
                return False
            transaction.held_locks.add((self.table, base_rid))

        base_page_directory_entry = self.table.page_directory[base_rid]
        base_page_range_number = base_page_directory_entry.page_range_number
        base_page_range = self.table.page_range_directory[base_page_range_number]
        base_data_locations = base_page_directory_entry.data_locations

        if base_data_locations[table.INDIRECTION_COLUMN] is None:
            base_indirection_page_number = len(base_page_range.base_pages[table.INDIRECTION_COLUMN]) - 1
            base_indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][base_indirection_page_number]
            base_indirection_offset = base_indirection_page.current_offset
        else:
            base_indirection_page_number = base_data_locations[table.INDIRECTION_COLUMN].page_number
            base_indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][base_indirection_page_number]
            base_indirection_offset = base_data_locations[table.INDIRECTION_COLUMN].offset

        base_indirection = base_indirection_page.read(base_indirection_offset // page.COLUMN_ENTRY_SIZE)

        if transaction:
            base_schema_page_number = base_data_locations[table.SCHEMA_ENCODING_COLUMN].page_number
            base_schema_page = base_page_range.base_pages[table.SCHEMA_ENCODING_COLUMN][base_schema_page_number]
            base_schema_offset = base_data_locations[table.SCHEMA_ENCODING_COLUMN].offset
            base_schema_int = base_schema_page.read(base_schema_offset // page.COLUMN_ENTRY_SIZE)
            transaction.rollback_log.append(("update",self.table, base_rid, base_indirection, base_schema_int))

        base_schema_page_number = base_data_locations[table.SCHEMA_ENCODING_COLUMN].page_number
        base_schema_page = base_page_range.base_pages[table.SCHEMA_ENCODING_COLUMN][base_schema_page_number]
        base_schema_offset = base_data_locations[table.SCHEMA_ENCODING_COLUMN].offset
        base_schema_int = base_schema_page.read(base_schema_offset // page.COLUMN_ENTRY_SIZE)

        schema_int = 0
        new_base_schema_int = base_schema_int

        for i, v in enumerate(columns):
            if v is not None:
                bit_mask = 1 << (self.table.num_columns - 1 - i)
                schema_int |= bit_mask
                new_base_schema_int |= bit_mask

        if all(c is None for c in columns):
            new_base_schema_int = 0
            schema_int = 0

        base_schema_is_zero = (base_schema_int == 0)
        copy_tail_record = None

        if base_schema_is_zero and any(c is not None for c in columns):
            copy_columns = [None] * self.table.num_columns

            for i in range(self.table.num_columns):
                base_loc = base_data_locations[i + 3]
            bp = base_page_range.base_pages[i + 3][base_loc.page_number]
            copy_columns[i] = bp.read(base_loc.offset // page.COLUMN_ENTRY_SIZE)

            copy_tail_record = Record(next(_rid_counter), primary_key, copy_columns)

            full_schema_int = (1 << self.table.num_columns) - 1
            copy_all_columns = [copy_tail_record.rid, base_rid, full_schema_int] + copy_columns
            self.table.add_record(base_page_range_number, False, *copy_all_columns, record=copy_tail_record)

        tail_record = Record(next(_rid_counter), primary_key, list(columns))
        tail_indirection = copy_tail_record.rid if (base_schema_is_zero and copy_tail_record) else base_indirection

        if all(c is None for c in columns):
            tail_indirection = base_rid

        all_columns = [tail_record.rid, tail_indirection, schema_int] + list(columns)
        self.table.add_record(base_page_range_number, False, *all_columns, record=tail_record)

        base_indirection_page.write(tail_record.rid, base_indirection_offset)
        base_schema_page.write(new_base_schema_int, base_schema_offset)

        with self.table.directory_lock:
            base_page_directory_entry.data_locations[table.INDIRECTION_COLUMN] = table.PageCoord(
                base_indirection_page_number, base_indirection_offset
            )

        for i, new_val in enumerate(columns):
            if new_val is not None:
                prev_val = self.table.get_column_value(base_rid, i, 1)
                if prev_val is not None and new_val != prev_val:
                    if transaction:
                        transaction.rollback_log.append(
                    ("index_update", self.table, base_rid, i, prev_val, new_val)
                )
                    self.table.index.remove_from_index(i, prev_val, base_rid)
                    self.table.index.add_to_index(i, new_val, base_rid)

        self.table.trigger_merge_check()
        return True

    def sum(self, start_range, end_range, aggregate_column_index, transaction=None):
        result_sum = 0
        has_records = False

        for key in range(start_range, end_range + 1):
            rids = self.table.index.locate(self.table.key, key)
            if rids is None or len(rids) == 0:
                continue

            rid = rids[0]
            if rid not in self.table.page_directory:
                continue

            if transaction:
                if not self.table.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks.add((self.table, rid))

            has_records = True
            column_value = self.table.get_column_value(rid, aggregate_column_index, 0)
            if column_value is None:
                column_value = 0
            result_sum += column_value

        if not has_records:
            return False

        return result_sum

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, transaction=None):
        result_sum = 0
        has_records = False

        for key in range(start_range, end_range + 1):
            rids = self.table.index.locate(self.table.key, key)
            if rids is None or len(rids) == 0:
                continue

            rid = rids[0]

            if transaction:
                if not self.table.lock_manager.acquire_lock(rid, transaction.transaction_id, 'S'):
                    return False
                transaction.held_locks.add((self.table, rid))

            has_records = True
            column_value = self.table.get_column_value(rid, aggregate_column_index, -relative_version)
            if column_value is None:
                column_value = 0
            result_sum += column_value

        if not has_records:
            return False

        return result_sum

    def increment(self, key, column, transaction=None):
        r_list = self.select(key, self.table.key, [1] * self.table.num_columns, transaction=transaction)
        if r_list is not False and r_list:
            r = r_list[0]
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            return self.update(key, *updated_columns, transaction=transaction)
        return False