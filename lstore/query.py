from itertools import count

from lstore import table
from lstore import page
from lstore.table import Record

_rid_counter = count(1)


class Query:
    def __init__(self, table):
        self.table = table

    def insert(self, *columns, transaction=None):
        existing = self.table.index.locate(self.table.key, columns[self.table.key])
        if existing:
            return False

        if len(columns) != self.table.num_columns:
            return False

        primary_key = columns[self.table.key]
        page_range_number = primary_key // table.NUM_RECORDS_PER_RANGE

        if page_range_number not in self.table.page_range_directory:
            self.table.add_page_range(page_range_number)

        rid = next(_rid_counter)

        if transaction:
            if not self.table.lock_manager.acquire_lock(rid, transaction.transaction_id, "X"):
                return False
            transaction.held_locks.add((self.table, rid))

        record = Record(rid, primary_key, list(columns))
        all_columns = [rid, rid, 0] + list(columns)
        self.table.add_record(page_range_number, True, *all_columns, record=record)

        for i, value in enumerate(columns):
            self.table.index.add_to_index(i, value, rid)

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
                for rid, entry in self.table.page_directory.items():
                    if not entry.is_base:
                        continue
                    value = self.table.get_column_value(rid, search_key_index, 0)
                    if value == search_key:
                        rid_list.append(rid)

        records = []
        for rid in rid_list:
            if rid not in self.table.page_directory:
                continue

            if transaction:
                if not self.table.lock_manager.acquire_lock(rid, transaction.transaction_id, "S"):
                    return False
                transaction.held_locks.add((self.table, rid))

            columns = self.table.construct_full_record(rid, 0)
            primary_key = self.table.get_primary_key(rid)

            result_columns = []
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    result_columns.append(columns[i])

            records.append(Record(rid, primary_key, result_columns))

        return records

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction=None):
        version = -relative_version
        rid_list = self.table.index.locate(search_key_index, search_key)

        if not rid_list:
            rid_list = []
            with self.table.directory_lock:
                for rid, entry in self.table.page_directory.items():
                    if not entry.is_base:
                        continue
                    value = self.table.get_column_value(rid, search_key_index, version)
                    if value == search_key:
                        rid_list.append(rid)

        records = []
        for rid in rid_list:
            if rid not in self.table.page_directory:
                continue

            if transaction:
                if not self.table.lock_manager.acquire_lock(rid, transaction.transaction_id, "S"):
                    return False
                transaction.held_locks.add((self.table, rid))

            columns = self.table.construct_full_record(rid, version)
            primary_key = self.table.get_primary_key(rid)

            result_columns = []
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    result_columns.append(columns[i])

            records.append(Record(rid, primary_key, result_columns))

        return records

    def delete(self, primary_key, transaction=None):
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids:
            return False

        base_rid = rids[0]

        if transaction:
            if not self.table.lock_manager.acquire_lock(base_rid, transaction.transaction_id, "X"):
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
            if not self.table.lock_manager.acquire_lock(base_rid, transaction.transaction_id, "X"):
                return False
            transaction.held_locks.add((self.table, base_rid))

        base_entry = self.table.page_directory[base_rid]
        page_range_number = base_entry.page_range_number
        page_range = self.table.page_range_directory[page_range_number]
        data_locations = base_entry.data_locations

        indirection_loc = data_locations[table.INDIRECTION_COLUMN]
        if indirection_loc is None:
            indirection_page_number = len(page_range.base_pages[table.INDIRECTION_COLUMN]) - 1
            indirection_page = page_range.base_pages[table.INDIRECTION_COLUMN][indirection_page_number]
            indirection_offset = indirection_page.current_offset
        else:
            indirection_page_number = indirection_loc.page_number
            indirection_page = page_range.base_pages[table.INDIRECTION_COLUMN][indirection_page_number]
            indirection_offset = indirection_loc.offset

        old_indirection = indirection_page.read(indirection_offset // page.COLUMN_ENTRY_SIZE)

        schema_loc = data_locations[table.SCHEMA_ENCODING_COLUMN]
        schema_page_number = schema_loc.page_number
        schema_page = page_range.base_pages[table.SCHEMA_ENCODING_COLUMN][schema_page_number]
        schema_offset = schema_loc.offset
        old_schema = schema_page.read(schema_offset // page.COLUMN_ENTRY_SIZE)

        if transaction:
            transaction.rollback_log.append(
                ("update", self.table, base_rid, old_indirection, old_schema)
            )

        schema_int = 0
        new_base_schema = old_schema

        for i, value in enumerate(columns):
            if value is not None:
                bit_mask = 1 << (self.table.num_columns - 1 - i)
                schema_int |= bit_mask
                new_base_schema |= bit_mask

        if all(value is None for value in columns):
            schema_int = 0
            new_base_schema = 0

        base_schema_is_zero = (old_schema == 0)
        copy_tail_record = None

        if base_schema_is_zero and any(value is not None for value in columns):
            copy_columns = [None] * self.table.num_columns

            for i in range(self.table.num_columns):
                base_loc = data_locations[i + 3]
                base_page = page_range.base_pages[i + 3][base_loc.page_number]
                copy_columns[i] = base_page.read(base_loc.offset // page.COLUMN_ENTRY_SIZE)

            copy_tail_record = Record(next(_rid_counter), primary_key, copy_columns)
            full_schema = (1 << self.table.num_columns) - 1
            copy_all_columns = [copy_tail_record.rid, base_rid, full_schema] + copy_columns
            self.table.add_record(page_range_number, False, *copy_all_columns, record=copy_tail_record)

        tail_record = Record(next(_rid_counter), primary_key, list(columns))
        tail_indirection = copy_tail_record.rid if (base_schema_is_zero and copy_tail_record) else old_indirection

        if all(value is None for value in columns):
            tail_indirection = base_rid

        all_columns = [tail_record.rid, tail_indirection, schema_int] + list(columns)
        self.table.add_record(page_range_number, False, *all_columns, record=tail_record)

        indirection_page.write(tail_record.rid, indirection_offset)
        schema_page.write(new_base_schema, schema_offset)

        with self.table.directory_lock:
            base_entry.data_locations[table.INDIRECTION_COLUMN] = table.PageCoord(
                indirection_page_number, indirection_offset
            )

        for i, new_value in enumerate(columns):
            if new_value is not None:
                old_value = self.table.get_column_value(base_rid, i, 1)
                if old_value is not None and new_value != old_value:
                    if transaction:
                        transaction.rollback_log.append(
                            ("index_update", self.table, base_rid, i, old_value, new_value)
                        )
                    self.table.index.remove_from_index(i, old_value, base_rid)
                    self.table.index.add_to_index(i, new_value, base_rid)

        self.table.trigger_merge_check()
        return True

    def sum(self, start_range, end_range, aggregate_column_index, transaction=None):
        total = 0
        found = False

        for key in range(start_range, end_range + 1):
            rids = self.table.index.locate(self.table.key, key)
            if not rids:
                continue

            rid = rids[0]
            if rid not in self.table.page_directory:
                continue

            if transaction:
                if not self.table.lock_manager.acquire_lock(rid, transaction.transaction_id, "S"):
                    return False
                transaction.held_locks.add((self.table, rid))

            found = True
            value = self.table.get_column_value(rid, aggregate_column_index, 0)
            if value is None:
                value = 0
            total += value

        if not found:
            return False

        return total

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, transaction=None):
        total = 0
        found = False
        version = -relative_version

        for key in range(start_range, end_range + 1):
            rids = self.table.index.locate(self.table.key, key)
            if not rids:
                continue

            rid = rids[0]
            if rid not in self.table.page_directory:
                continue

            if transaction:
                if not self.table.lock_manager.acquire_lock(rid, transaction.transaction_id, "S"):
                    return False
                transaction.held_locks.add((self.table, rid))

            found = True
            value = self.table.get_column_value(rid, aggregate_column_index, version)
            if value is None:
                value = 0
            total += value

        if not found:
            return False

        return total

    def increment(self, key, column, transaction=None):
        result = self.select(key, self.table.key, [1] * self.table.num_columns, transaction=transaction)
        if result is not False and result:
            record = result[0]
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = record.columns[column] + 1
            return self.update(key, *updated_columns, transaction=transaction)
        return False

