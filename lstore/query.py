from lstore import table
from lstore import page
from lstore.table import Record
from itertools import count

_rid_counter = count(1)


class Query:
    def __init__(self, table):
        self.table = table

    def delete(self, primary_key):
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids:
            return False
        base_rid = rids[0]

        if not self.update(primary_key, *([None] * self.table.num_columns)):
            return False

        if base_rid in self.table.page_directory:
            del self.table.page_directory[base_rid]
        return True

    def insert(self, *columns):
        if len(columns) != self.table.num_columns:
            return False

        existing = self.table.index.locate(self.table.key, columns[self.table.key])
        if existing is not None and len(existing) > 0:
            return False

        primary_key = columns[self.table.key]
        page_range_number = primary_key // table.NUM_RECORDS_PER_RANGE

        if page_range_number not in self.table.page_range_directory:
            self.table.add_page_range(page_range_number)

        rid = next(_rid_counter)
        record = Record(rid, primary_key, list(columns))

        all_columns = [rid, None, 0] + list(columns)
        self.table.add_record(page_range_number, True, *all_columns, record=record)

        for i, v in enumerate(columns):
            self.table.index.add_to_index(i, v, rid)

        return True

    def select(self, search_key, search_key_index, projected_columns_index):
        rid_list = self.table.index.locate(search_key_index, search_key)

        if rid_list is None:
            rid_list = []
            for rid, entry in self.table.page_directory.items():
                if not entry.is_base:
                    continue
                cols = self.table.construct_full_record(rid)
                if cols[search_key_index] == search_key:
                    rid_list.append(rid)

        out = []
        for rid in rid_list:
            if rid not in self.table.page_directory:
                continue
            cols = self.table.construct_full_record(rid)
            pk = self.table.get_primary_key(rid)
            proj = [cols[i] for i, keep in enumerate(projected_columns_index) if keep]
            out.append(Record(rid, pk, proj))
        return out

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rid_list = self.table.index.locate(search_key_index, search_key)

        if rid_list is None:
            rid_list = []
            for rid, entry in self.table.page_directory.items():
                if not entry.is_base:
                    continue
                cols = self.table.construct_full_record(rid)
                if cols[search_key_index] == search_key:
                    rid_list.append(rid)

        out = []
        for rid in rid_list:
            if rid not in self.table.page_directory:
                continue
            cols = self.table.construct_full_record(rid, relative_version * -1)
            pk = self.table.get_primary_key(rid)
            proj = [cols[i] for i, keep in enumerate(projected_columns_index) if keep]
            out.append(Record(rid, pk, proj))
        return out

    def update(self, primary_key, *columns):
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids:
            return False
        base_rid = rids[0]

        if columns[self.table.key] is not None:
            return False

        cols = list(columns)
        all_none = all(v is None for v in cols)

        base_entry = self.table.page_directory[base_rid]
        pr_num = base_entry.page_range_number
        pr = self.table.page_range_directory[pr_num]
        locs = base_entry.data_locations

        schema_loc = locs[table.SCHEMA_ENCODING_COLUMN]
        schema_page = pr.base_pages[table.SCHEMA_ENCODING_COLUMN][schema_loc.page_number]
        schema_off = schema_loc.offset
        base_schema_int = schema_page.read(schema_off // page.COLUMN_ENTRY_SIZE)

        ind_loc = locs[table.INDIRECTION_COLUMN]
        if ind_loc is None:
            ind_page_num = len(pr.base_pages[table.INDIRECTION_COLUMN]) - 1
            ind_page = pr.base_pages[table.INDIRECTION_COLUMN][ind_page_num]
            ind_off = ind_page.current_offset
        else:
            ind_page_num = ind_loc.page_number
            ind_page = pr.base_pages[table.INDIRECTION_COLUMN][ind_page_num]
            ind_off = ind_loc.offset

        base_indirection = ind_page.read(ind_off // page.COLUMN_ENTRY_SIZE)

        schema_int = 0
        new_base_schema_int = base_schema_int
        for i, v in enumerate(cols):
            if v is not None:
                bit = 1 << (self.table.num_columns - 1 - i)
                schema_int |= bit
                new_base_schema_int |= bit

        if all_none:
            schema_int = 0
            new_base_schema_int = 0

        base_schema_is_zero = (base_schema_int == 0)
        copy_tail = None

        if base_schema_is_zero and not all_none:
            copy_cols = [None] * self.table.num_columns
            for i, v in enumerate(cols):
                if v is not None:
                    dl = locs[i + 3]
                    bp = pr.base_pages[i + 3][dl.page_number]
                    copy_cols[i] = bp.read(dl.offset // page.COLUMN_ENTRY_SIZE)
            copy_tail = Record(next(_rid_counter), primary_key, copy_cols)
            self.table.add_record(pr_num, False, *( [copy_tail.rid, base_rid, schema_int] + copy_cols ), record=copy_tail)

        tail = Record(next(_rid_counter), primary_key, cols)

        if all_none:
            tail_ind = base_rid
        else:
            tail_ind = copy_tail.rid if (base_schema_is_zero and copy_tail is not None) else base_indirection

        self.table.add_record(pr_num, False, *( [tail.rid, tail_ind, schema_int] + cols ), record=tail)

        if ind_off == page.PAGE_SIZE:
            ind_page_num += 1
            pr.add_page(True, table.INDIRECTION_COLUMN)
            ind_page = pr.base_pages[table.INDIRECTION_COLUMN][ind_page_num]
            ind_off = 0

        ind_page.write(tail.rid, ind_off)
        schema_page.write(new_base_schema_int, schema_off)
        base_entry.data_locations[table.INDIRECTION_COLUMN] = table.PageCoord(ind_page_num, ind_off)

        if all_none:
            self.table.index.remove_from_index(self.table.key, primary_key, base_rid)
            self.table.trigger_merge_check()
            return True

        for i, new_val in enumerate(cols):
            if new_val is None:
                continue
            prev_val = self.table.get_prev_value_for_index(base_rid, i)
            if prev_val is not None and prev_val != new_val:
                self.table.index.remove_from_index(i, prev_val, base_rid)
                self.table.index.add_to_index(i, new_val, base_rid)

        self.table.trigger_merge_check()
        return True

    def sum(self, start_range, end_range, aggregate_column_index):
        total = 0
        found = False
        for key in range(start_range, end_range + 1):
            rids = self.table.index.locate(self.table.key, key)
            if not rids:
                continue
            rid = rids[0]
            if rid not in self.table.page_directory:
                continue
            found = True
            v = self.table.get_column_value(rid, aggregate_column_index)
            total += 0 if v is None else v
        return total if found else False

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        total = 0
        found = False
        for key in range(start_range, end_range + 1):
            rids = self.table.index.locate(self.table.key, key)
            if not rids:
                continue
            rid = rids[0]
            found = True
            v = self.table.get_column_value(rid, aggregate_column_index, relative_version * -1)
            total += 0 if v is None else v
        return total if found else False

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is False:
            return False
        updated = [None] * self.table.num_columns
        updated[column] = r[column] + 1
        return self.update(key, *updated)