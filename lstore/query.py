from lstore import table
from lstore import page
from lstore.table import Table, Record
from lstore.index import Index
from itertools import count

_rid_counter = count(1)


class Query:
    """
    Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table

    """
    Read a record with specified RID
    Returns True upon succesful deletion
    Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids:
            return False
        base_rid = rids[0]

        # Tombstone update (all None)
        ok = self.update(primary_key, *([None] * self.table.num_columns))
        if not ok:
            return False

        # Remove base RID from page directory
        if base_rid in self.table.page_directory:
            del self.table.page_directory[base_rid]
        return True

    """
    Insert a record with specified columns
    Return True upon succesful insertion
    Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
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

        schema_encoding = '0' * self.table.num_columns
        record = Record(rid, primary_key, list(columns))

        all_columns = [rid, None, int(schema_encoding, 2)] + list(columns)
        self.table.add_record(page_range_number, True, *all_columns, record=record)

        for i, column in enumerate(columns):
            self.table.index.add_to_index(i, column, rid)

        return True

    """
    Read matching record with specified search key
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        rid_list = self.table.index.locate(search_key_index, search_key)

        # If no index exists on this column, fall back to full table scan
        if rid_list is None:
            rid_list = []
            for rid, entry in self.table.page_directory.items():
                if not entry.is_base:
                    continue
                columns = self.table.construct_full_record(rid)
                if columns[search_key_index] == search_key:
                    rid_list.append(rid)

        record_list = []
        for rid in rid_list:
            if rid not in self.table.page_directory:
                continue
            columns = self.table.construct_full_record(rid)
            primary_key = self.table.get_primary_key(rid)

            new_columns = []
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 0:
                    continue
                new_columns.append(columns[i])

            record_list.append(Record(rid, primary_key, new_columns))

        return record_list

    """
    Read matching record with specified search key (versioned)
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rid_list = self.table.index.locate(search_key_index, search_key)

        if rid_list is None:
            rid_list = []
            for rid, entry in self.table.page_directory.items():
                if not entry.is_base:
                    continue
                columns = self.table.construct_full_record(rid)
                if columns[search_key_index] == search_key:
                    rid_list.append(rid)

        record_list = []
        for rid in rid_list:
            if rid not in self.table.page_directory:
                continue

            columns = self.table.construct_full_record(rid, relative_version * -1)
            primary_key = self.table.get_primary_key(rid)

            new_columns = []
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 0:
                    continue
                new_columns.append(columns[i])

            record_list.append(Record(rid, primary_key, new_columns))

        return record_list

    """
    Update a record with specified key and columns
    Returns True if update is succesful
    Returns False if no records exist with given key
    """
    def update(self, primary_key, *columns):
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids:
            return False
        base_rid = rids[0]

        # PK cannot be updated
        if columns[self.table.key] is not None:
            return False

        # FIX: columns is tuple -> convert once
        cols = list(columns)
        all_none = all(v is None for v in cols)

        base_entry = self.table.page_directory[base_rid]
        base_page_range_number = base_entry.page_range_number
        base_page_range = self.table.page_range_directory[base_page_range_number]
        base_data_locations = base_entry.data_locations

        # Base schema
        base_schema_page_number = base_data_locations[table.SCHEMA_ENCODING_COLUMN].page_number
        base_schema_page = base_page_range.base_pages[table.SCHEMA_ENCODING_COLUMN][base_schema_page_number]
        base_schema_offset = base_data_locations[table.SCHEMA_ENCODING_COLUMN].offset
        base_schema_int = base_schema_page.read(base_schema_offset // page.COLUMN_ENTRY_SIZE)

        # Base indirection
        if base_data_locations[table.INDIRECTION_COLUMN] is None:
            base_indirection_page_number = len(base_page_range.base_pages[table.INDIRECTION_COLUMN]) - 1
            base_indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][base_indirection_page_number]
            base_indirection_offset = base_indirection_page.current_offset
        else:
            base_indirection_page_number = base_data_locations[table.INDIRECTION_COLUMN].page_number
            base_indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][base_indirection_page_number]
            base_indirection_offset = base_data_locations[table.INDIRECTION_COLUMN].offset

        base_indirection = base_indirection_page.read(base_indirection_offset // page.COLUMN_ENTRY_SIZE)

        # Compute schema changes
        schema_int = 0
        new_base_schema_int = base_schema_int

        for i, v in enumerate(cols):
            if v is not None:
                bit_mask = 1 << (self.table.num_columns - 1 - i)
                schema_int |= bit_mask
                new_base_schema_int |= bit_mask

        # If delete-like update, schema becomes 0
        if all_none:
            schema_int = 0
            new_base_schema_int = 0

        base_schema_is_zero = (base_schema_int == 0)

        copy_tail_record = None

        # First update: create copy tail record (only if NOT delete)
        if base_schema_is_zero and (not all_none):
            copy_columns = [None] * self.table.num_columns
            for i, column in enumerate(cols):
                if column is not None:
                    base_page_number = base_data_locations[i + 3].page_number
                    base_page = base_page_range.base_pages[i + 3][base_page_number]
                    base_offset = base_data_locations[i + 3].offset
                    copy_columns[i] = base_page.read(base_offset // page.COLUMN_ENTRY_SIZE)
                else:
                    copy_columns[i] = None

            copy_tail_record = Record(next(_rid_counter), primary_key, copy_columns)
            copy_all_columns = [copy_tail_record.rid, base_rid, schema_int] + copy_columns
            self.table.add_record(base_page_range_number, False, *copy_all_columns, record=copy_tail_record)

        # Create tail record
        tail_record = Record(next(_rid_counter), primary_key, cols)

        if all_none:
            tail_indirection = base_rid
        else:
            tail_indirection = copy_tail_record.rid if (base_schema_is_zero and copy_tail_record is not None) else base_indirection

        all_columns = [tail_record.rid, tail_indirection, schema_int] + cols
        self.table.add_record(base_page_range_number, False, *all_columns, record=tail_record)

        # Ensure indirection page has space
        if base_indirection_offset == page.PAGE_SIZE:
            base_indirection_page_number += 1
            base_page_range.add_page(True, table.INDIRECTION_COLUMN)
            base_indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][base_indirection_page_number]
            base_indirection_offset = 0

        # Update base indirection + schema
        base_indirection_page.write(tail_record.rid, base_indirection_offset)
        base_schema_page.write(new_base_schema_int, base_schema_offset)

        base_entry.data_locations[table.INDIRECTION_COLUMN] = table.PageCoord(base_indirection_page_number, base_indirection_offset)

        # Optimization: if delete-like update, skip per-column prev_val calls
        if all_none:
            # Remove PK from index for faster deletes
            self.table.index.remove_from_index(self.table.key, primary_key, base_rid)
            self.table.trigger_merge_check()
            return True

        # Update indexes only for changed columns
        for i, new_val in enumerate(cols):
            if new_val is not None:
                prev_val = self.table.get_column_value(base_rid, i, 1)
                if prev_val is not None and new_val != prev_val:
                    self.table.index.remove_from_index(i, prev_val, base_rid)
                    self.table.index.add_to_index(i, new_val, base_rid)

        self.table.trigger_merge_check()
        return True

    """
    Sum aggregation over primary key range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        total = 0
        has_records = False

        for key in range(start_range, end_range + 1):
            rids = self.table.index.locate(self.table.key, key)
            if not rids:
                continue
            rid = rids[0]
            if rid not in self.table.page_directory:
                continue

            has_records = True
            column_value = self.table.get_column_value(rid, aggregate_column_index)
            if column_value is None:
                column_value = 0
            total += column_value

        if not has_records:
            return False
        return total

    """
    Sum aggregation over primary key range (versioned)
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        total = 0
        has_records = False

        for key in range(start_range, end_range + 1):
            rids = self.table.index.locate(self.table.key, key)
            if not rids:
                continue
            rid = rids[0]

            has_records = True
            column_value = self.table.get_column_value(rid, aggregate_column_index, relative_version * -1)
            if column_value is None:
                column_value = 0
            total += column_value

        if not has_records:
            return False
        return total

    """
    Increments one column of the record
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False