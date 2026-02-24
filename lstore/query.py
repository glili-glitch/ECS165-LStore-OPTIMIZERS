from lstore import table
from lstore import page
from lstore.table import Table, Record
from itertools import count


_rid_counter = count(1)


class Query:
    """
    Query object for a specific table.

    
    """

    def __init__(self, table: Table):
        self.table = table

    # ----------------------------
    # Helper: get matching base RIDs
    # ----------------------------
    def _get_rids(self, search_key, search_key_index):
        """
        Use index if available, otherwise scan base records.
        This is what fixes "select without index" tests.
        """
        # 1) Try index first 
        if self.table.index is not None:
            rids = self.table.index.locate(search_key_index, search_key)
            if rids is not None and len(rids) > 0:
                return list(rids)

        # 2) Fallback: full scan over base records
        rids = []
        for rid, entry in self.table.page_directory.items():
            if not entry.is_base:
                continue
            if rid in self.table.deleted_rids:
                continue

            cols = self.table.construct_full_record(rid)
            if cols[search_key_index] == search_key:
                rids.append(rid)

        return rids

    # ----------------------------
    # Delete
    # ----------------------------
    def delete(self, primary_key):
        """
        Logical delete:
        - find base rid
        - remove rid from index mappings (if index exists)
        - mark rid as deleted
        """
        rids = self._get_rids(primary_key, self.table.key)
        if not rids:
            return False

        base_rid = rids[0]
        if base_rid in self.table.deleted_rids:
            return False

        # remove from all indexes (if any)
        if self.table.index is not None:
            current_cols = self.table.construct_full_record(base_rid)
            for i, val in enumerate(current_cols):
                self.table.index.remove_from_index(i, val, base_rid)

        self.table.deleted_rids.add(base_rid)
        return True

    # ----------------------------
    # Insert
    # ----------------------------
    def insert(self, *columns):
        if len(columns) != self.table.num_columns:
            return False

        primary_key = columns[self.table.key]

        # primary key uniqueness (works with or without index)
        if len(self._get_rids(primary_key, self.table.key)) > 0:
            return False

        # pick page range based on key
        page_range_number = primary_key // table.NUM_RECORDS_PER_RANGE
        if page_range_number not in self.table.page_range_directory:
            self.table.add_page_range(page_range_number)

        # allocate RID
        rid = next(_rid_counter)

        schema_encoding = "0" * self.table.num_columns

        record = Record(rid, primary_key, list(columns))
        all_columns = [rid, None, int(schema_encoding, 2)] + list(columns)

        self.table.add_record(page_range_number, True, *all_columns, record=record)

        # update index mappings if index exists
        if self.table.index is not None:
            for i, val in enumerate(columns):
                self.table.index.add_to_index(i, val, rid)

        return True

    # ----------------------------
    # Select (must work WITHOUT index)
    # ----------------------------
    def select(self, search_key, search_key_index, projected_columns_index):
        rid_list = self._get_rids(search_key, search_key_index)
        record_list = []

        for rid in rid_list:
            if rid in self.table.deleted_rids:
                continue

            cols = self.table.construct_full_record(rid)

            # IMPORTANT FIX:
            # primary key must come from the latest reconstructed columns (after updates)
            pk = cols[self.table.key]

            projected = []
            for i, keep in enumerate(projected_columns_index):
                if keep:
                    projected.append(cols[i])

            record_list.append(Record(rid, pk, projected))

        return record_list

    # ----------------------------
    # Select Version (must work WITHOUT index)
    # ----------------------------
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rid_list = self._get_rids(search_key, search_key_index)
        record_list = []

        for rid in rid_list:
            if rid in self.table.deleted_rids:
                continue

            cols = self.table.construct_full_record(rid, relative_version * -1)
            pk = cols[self.table.key]

            projected = []
            for i, keep in enumerate(projected_columns_index):
                if keep:
                    projected.append(cols[i])

            record_list.append(Record(rid, pk, projected))

        return record_list

    # ----------------------------
    # Update (your logic, with key-change + delete handling improved)
    # ----------------------------
    def update(self, primary_key, *columns):
        rids = self._get_rids(primary_key, self.table.key)
        if not rids:
            return False
        base_rid = rids[0]

        # don't update deleted records
        if base_rid in self.table.deleted_rids:
            return False

        # if primary key is being changed, new key must be unique
        if columns[self.table.key] is not None:
            new_key = columns[self.table.key]
            existing = self._get_rids(new_key, self.table.key)
            if len(existing) > 0 and existing[0] != base_rid:
                return False

        base_page_directory_entry = self.table.page_directory[base_rid]
        base_page_range_number = base_page_directory_entry.page_range_number
        base_page_range = self.table.page_range_directory[base_page_range_number]
        base_data_locations = base_page_directory_entry.data_locations

        # locate base indirection
        if base_data_locations[table.INDIRECTION_COLUMN] is None:
            base_indirection_page_number = len(base_page_range.base_pages[table.INDIRECTION_COLUMN]) - 1
            base_indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][base_indirection_page_number]
            base_indirection_offset = base_indirection_page.current_offset
        else:
            base_indirection_page_number = base_data_locations[table.INDIRECTION_COLUMN].page_number
            base_indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][base_indirection_page_number]
            base_indirection_offset = base_data_locations[table.INDIRECTION_COLUMN].offset

        # base schema encoding
        base_schema_page_number = base_data_locations[table.SCHEMA_ENCODING_COLUMN].page_number
        base_schema_page = base_page_range.base_pages[table.SCHEMA_ENCODING_COLUMN][base_schema_page_number]
        base_schema_offset = base_data_locations[table.SCHEMA_ENCODING_COLUMN].offset
        base_schema = format(
            base_schema_page.read(base_schema_offset // page.COLUMN_ENTRY_SIZE),
            f"0{self.table.num_columns}b",
        )

        base_indirection = base_indirection_page.read(base_indirection_offset // page.COLUMN_ENTRY_SIZE)

        # build schema for tail + update base schema
        schema = ""
        new_base_schema = base_schema
        for i, v in enumerate(columns):
            if v is None:
                schema += "0"
            else:
                schema += "1"
                new_base_schema = new_base_schema[:i] + "1" + new_base_schema[i + 1 :]

        # delete-style update: all None
        if list(columns) == [None] * self.table.num_columns:
            new_base_schema = "0" * self.table.num_columns
            schema = "0" * self.table.num_columns

        copy_tail_record = None

        # first update: create copy tail for updated columns
        if base_schema == "0" * self.table.num_columns and list(columns) != [None] * self.table.num_columns:
            copy_columns = [None] * self.table.num_columns
            for i, column in enumerate(columns):
                if column is not None:
                    base_page_number = base_data_locations[i + 3].page_number
                    base_page = base_page_range.base_pages[i + 3][base_page_number]
                    base_offset = base_data_locations[i + 3].offset
                    column_value = base_page.read(base_offset // page.COLUMN_ENTRY_SIZE)
                    copy_columns[i] = column_value
                else:
                    copy_columns[i] = None

            copy_tail_record = Record(next(_rid_counter), primary_key, copy_columns)
            copy_all_columns = [copy_tail_record.rid, base_rid, int(schema, 2)] + copy_columns
            self.table.add_record(base_page_range_number, False, *copy_all_columns, record=copy_tail_record)

        # create main tail record
        tail_record = Record(next(_rid_counter), primary_key, list(columns))

        if base_schema == "0" * self.table.num_columns and list(columns) != [None] * self.table.num_columns:
            tail_indirection = copy_tail_record.rid
        else:
            tail_indirection = base_indirection

        if list(columns) == [None] * self.table.num_columns:
            tail_indirection = base_rid

        all_columns = [tail_record.rid, tail_indirection, int(schema, 2)] + list(columns)
        self.table.add_record(base_page_range_number, False, *all_columns, record=tail_record)

        # ensure base indirection page has room
        if base_indirection_offset == page.PAGE_SIZE:
            base_indirection_page_number += 1
            base_page_range.add_page(True, table.INDIRECTION_COLUMN)
            base_indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][base_indirection_page_number]
            base_indirection_offset = 0

        # write back base indirection and schema encoding
        base_indirection_page.write(tail_record.rid, base_indirection_offset)
        base_schema_page.write(int(new_base_schema, 2), base_schema_offset)

        base_page_directory_entry.data_locations[table.INDIRECTION_COLUMN] = table.PageCoord(
            base_indirection_page_number, base_indirection_offset
        )

        # index maintenance for changed values (including key changes)
        if self.table.index is not None:
            current_columns = self.table.construct_full_record(base_rid)
            previous_columns = self.table.construct_full_record(base_rid, 1)

            for i, (curr_val, prev_val) in enumerate(zip(current_columns, previous_columns)):
                if curr_val != prev_val:
                    if prev_val is not None:
                        self.table.index.remove_from_index(i, prev_val, base_rid)
                    if curr_val is not None:
                        self.table.index.add_to_index(i, curr_val, base_rid)

        # merge trigger (your existing behavior)
        self.table.trigger_merge_check()

        return True

    # ----------------------------
    # Sum / Sum Version (use _get_rids so it works w/out index)
    # ----------------------------
    def sum(self, start_range, end_range, aggregate_column_index):
        total = 0
        has_records = False

        for key in range(start_range, end_range + 1):
            rids = self._get_rids(key, self.table.key)
            if not rids:
                continue
            rid = rids[0]
            if rid in self.table.deleted_rids:
                continue

            has_records = True
            val = self.table.get_column_value(rid, aggregate_column_index)
            if val is None:
                val = 0
            total += val

        if not has_records:
            return False
        return total

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        total = 0
        has_records = False

        for key in range(start_range, end_range + 1):
            rids = self._get_rids(key, self.table.key)
            if not rids:
                continue
            rid = rids[0]
            if rid in self.table.deleted_rids:
                continue

            has_records = True
            val = self.table.get_column_value(rid, aggregate_column_index, relative_version * -1)
            if val is None:
                val = 0
            total += val

        if not has_records:
            return False
        return total

    # ----------------------------
    # Increment
    # ----------------------------
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        updated_columns = [None] * self.table.num_columns
        updated_columns[column] = r[column] + 1
        return self.update(key, *updated_columns)