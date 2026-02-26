import threading
from dataclasses import dataclass
from typing import List

from lstore import page
from lstore.index import Index
from lstore.page import Page, COLUMN_ENTRY_SIZE

RID_COLUMN = 0
INDIRECTION_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2

NUM_RECORDS_PER_RANGE = 1024


@dataclass
class PageCoord:
    page_number: int
    offset: int


@dataclass
class PageDirectoryEntry:
    page_range_number: int
    is_base: bool
    data_locations: List[PageCoord]


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.merge_threshold = 1000
        self.page_range_directory = {}
        self._update_count = 0
        self._merge_lock = threading.Lock()

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_merge_lock"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._merge_lock = threading.Lock()

    def get_primary_key(self, rid):
        e = self.page_directory[rid]
        pr = self.page_range_directory[e.page_range_number]
        rec = pr.get_record(e.is_base, rid)
        return rec.columns[self.key]

    def add_page_range(self, page_range_number):
        if page_range_number in self.page_range_directory:
            return False
        self.page_range_directory[page_range_number] = PageRange(self, page_range_number)
        return True

    def add_record(self, page_range_number, is_base, *all_columns, record):
        pr = self.page_range_directory[page_range_number]
        pages = pr.base_pages if is_base else pr.tail_pages
        locs = [None] * (self.num_columns + 3)

        for col in range(self.num_columns + 3):
            v = all_columns[col]
            if v is None:
                locs[col] = None
                continue

            last_page = pages[col][-1]
            page_num = len(pages[col]) - 1
            off = last_page.current_offset

            if not last_page.has_capacity():
                pr.add_page(is_base, col)
                page_num += 1
                off = 0

            locs[col] = PageCoord(page_num, off)
            pages[col][page_num].write(v)

        self.page_directory[record.rid] = PageDirectoryEntry(page_range_number, is_base, locs)
        pr.add_record(is_base, record)

    def construct_full_record(self, rid, relative_version=0):
        base_entry = self.page_directory[rid]
        pr = self.page_range_directory[base_entry.page_range_number]
        base_locs = base_entry.data_locations

        rid_loc = base_locs[RID_COLUMN]
        rid_page = pr.base_pages[RID_COLUMN][rid_loc.page_number]
        base_rid = rid_page.read(rid_loc.offset // page.COLUMN_ENTRY_SIZE)

        schema_loc = base_locs[SCHEMA_ENCODING_COLUMN]
        schema_page = pr.base_pages[SCHEMA_ENCODING_COLUMN][schema_loc.page_number]
        schema_int = schema_page.read(schema_loc.offset // page.COLUMN_ENTRY_SIZE)

        if schema_int == 0:
            return pr.get_record(True, rid).columns

        ind_loc = base_locs[INDIRECTION_COLUMN]
        ind_page = pr.base_pages[INDIRECTION_COLUMN][ind_loc.page_number]
        ind_rid = ind_page.read(ind_loc.offset // page.COLUMN_ENTRY_SIZE)

        cols = [None] * self.num_columns
        ver = 0

        while ind_rid is not None and ind_rid != base_rid:
            if relative_version == 0 and pr.tps > 0 and ind_rid <= pr.tps:
                break

            e = self.page_directory[ind_rid]
            pr2 = self.page_range_directory[e.page_range_number]
            rec = pr2.get_record(e.is_base, ind_rid)
            cur = rec.columns

            ind2 = e.data_locations[INDIRECTION_COLUMN]
            pages = pr2.base_pages if e.is_base else pr2.tail_pages
            ind_page2 = pages[INDIRECTION_COLUMN][ind2.page_number]
            ind_rid = ind_page2.read(ind2.offset // page.COLUMN_ENTRY_SIZE)

            if ver >= relative_version:
                cols = [x if x is not None else y for x, y in zip(cols, cur)]
            ver += 1

        if pr.tps > 0 and relative_version == 0:
            base_cols = []
            for c in range(self.num_columns):
                loc = base_locs[c + 3]
                if loc is None:
                    base_cols.append(None)
                else:
                    bp = pr.base_pages[c + 3][loc.page_number]
                    base_cols.append(bp.read(loc.offset // page.COLUMN_ENTRY_SIZE))
        else:
            base_cols = pr.get_record(True, base_rid).columns

        return [x if x is not None else y for x, y in zip(cols, base_cols)]

    def get_column_value(self, rid, column_number, relative_version=0):
        if relative_version != 0:
            return self.construct_full_record(rid, relative_version)[column_number]

        base_entry = self.page_directory[rid]
        pr = self.page_range_directory[base_entry.page_range_number]
        locs = base_entry.data_locations

        schema_loc = locs[SCHEMA_ENCODING_COLUMN]
        schema_page = pr.base_pages[SCHEMA_ENCODING_COLUMN][schema_loc.page_number]
        schema_int = schema_page.read(schema_loc.offset // COLUMN_ENTRY_SIZE)

        bit = 1 << (self.num_columns - 1 - column_number)
        updated = (schema_int & bit) != 0

        if not updated:
            loc = locs[column_number + 3]
            if loc is None:
                return None
            bp = pr.base_pages[column_number + 3][loc.page_number]
            return bp.read(loc.offset // COLUMN_ENTRY_SIZE)

        ind_loc = locs[INDIRECTION_COLUMN]
        ind_page = pr.base_pages[INDIRECTION_COLUMN][ind_loc.page_number]
        ind_rid = ind_page.read(ind_loc.offset // COLUMN_ENTRY_SIZE)

        rid_loc = locs[RID_COLUMN]
        rid_page = pr.base_pages[RID_COLUMN][rid_loc.page_number]
        base_rid = rid_page.read(rid_loc.offset // COLUMN_ENTRY_SIZE)

        while ind_rid is not None and ind_rid != base_rid:
            if pr.tps > 0 and ind_rid <= pr.tps:
                break

            e = self.page_directory.get(ind_rid)
            if e is None:
                break

            pr2 = self.page_range_directory[e.page_range_number]
            rec = pr2.get_record(e.is_base, ind_rid)
            if rec is not None:
                v = rec.columns[column_number]
                if v is not None:
                    return v

            ind2 = e.data_locations[INDIRECTION_COLUMN]
            pages = pr2.base_pages if e.is_base else pr2.tail_pages
            ind_page2 = pages[INDIRECTION_COLUMN][ind2.page_number]
            ind_rid = ind_page2.read(ind2.offset // page.COLUMN_ENTRY_SIZE)

        loc = locs[column_number + 3]
        if loc is None:
            return None
        bp = pr.base_pages[column_number + 3][loc.page_number]
        return bp.read(loc.offset // COLUMN_ENTRY_SIZE)

    def get_prev_value_for_index(self, base_rid, col):
        e = self.page_directory[base_rid]
        pr = self.page_range_directory[e.page_range_number]
        locs = e.data_locations

        schema_loc = locs[SCHEMA_ENCODING_COLUMN]
        schema_page = pr.base_pages[SCHEMA_ENCODING_COLUMN][schema_loc.page_number]
        schema_int = schema_page.read(schema_loc.offset // COLUMN_ENTRY_SIZE)

        bit = 1 << (self.num_columns - 1 - col)
        updated = (schema_int & bit) != 0

        if not updated:
            loc = locs[col + 3]
            if loc is None:
                return None
            bp = pr.base_pages[col + 3][loc.page_number]
            return bp.read(loc.offset // COLUMN_ENTRY_SIZE)

        ind_loc = locs[INDIRECTION_COLUMN]
        ind_page = pr.base_pages[INDIRECTION_COLUMN][ind_loc.page_number]
        ind_rid = ind_page.read(ind_loc.offset // COLUMN_ENTRY_SIZE)

        rid_loc = locs[RID_COLUMN]
        rid_page = pr.base_pages[RID_COLUMN][rid_loc.page_number]
        stop_rid = rid_page.read(rid_loc.offset // COLUMN_ENTRY_SIZE)

        while ind_rid is not None and ind_rid != stop_rid:
            if pr.tps > 0 and ind_rid <= pr.tps:
                break

            te = self.page_directory.get(ind_rid)
            if te is None:
                break

            tpr = self.page_range_directory[te.page_range_number]
            rec = tpr.get_record(te.is_base, ind_rid)
            if rec is not None:
                v = rec.columns[col]
                if v is not None:
                    return v

            ind2 = te.data_locations[INDIRECTION_COLUMN]
            pages = tpr.base_pages if te.is_base else tpr.tail_pages
            ip = pages[INDIRECTION_COLUMN][ind2.page_number]
            ind_rid = ip.read(ind2.offset // page.COLUMN_ENTRY_SIZE)

        loc = locs[col + 3]
        if loc is None:
            return None
        bp = pr.base_pages[col + 3][loc.page_number]
        return bp.read(loc.offset // COLUMN_ENTRY_SIZE)

    def trigger_merge_check(self):
        self._update_count += 1
        if self._update_count < self.merge_threshold:
            return
        self._update_count = 0

        results = []
        for pr_num in list(self.page_range_directory.keys()):
            r = self._merge_page_range(pr_num)
            if r is not None:
                results.append(r)

        for pr_num, copied_pages, max_tail_rid in results:
            pr = self.page_range_directory[pr_num]
            with self._merge_lock:
                pr.base_pages = copied_pages
                pr.tps = max_tail_rid

            if Page._bufferpool is not None:
                for col_pages in copied_pages:
                    for p in col_pages:
                        Page._bufferpool.access(p)
                        Page._bufferpool.mark_dirty(p.page_id)

    def _merge(self):
        out = []
        for pr_num in list(self.page_range_directory.keys()):
            r = self._merge_page_range(pr_num)
            if r is not None:
                out.append(r)
        return out

    def _merge_page_range(self, pr_num):
        pr = self.page_range_directory.get(pr_num)
        if pr is None or not pr.tail_records:
            return None

        max_tail_rid = max(pr.tail_records.keys())
        if max_tail_rid <= pr.tps:
            return None

        copied_pages = []
        for col in range(self.num_columns + 3):
            col_pages = []
            for orig in pr.base_pages[col]:
                newp = Page()
                newp.num_records = orig.num_records
                newp.current_offset = orig.current_offset
                orig._ensure_loaded()
                newp.data = bytearray(orig.data)
                col_pages.append(newp)
            copied_pages.append(col_pages)

        base_rids = list(pr.base_records.keys())
        for base_rid in base_rids:
            e = self.page_directory[base_rid]
            locs = e.data_locations
            latest = self.construct_full_record(base_rid)

            for col in range(self.num_columns):
                loc = locs[col + 3]
                if loc is not None and latest[col] is not None:
                    cp = copied_pages[col + 3][loc.page_number]
                    off = loc.offset
                    cp.data[off:off + COLUMN_ENTRY_SIZE] = latest[col].to_bytes(8, "little")

        return (pr_num, copied_pages, max_tail_rid)


class PageRange:
    def __init__(self, table, page_range_number):
        self.table = table
        self.page_range_number = page_range_number
        self.base_pages = [[Page()] for _ in range(table.num_columns + 3)]
        self.tail_pages = [[Page()] for _ in range(table.num_columns + 3)]
        self.base_records = {}
        self.tail_records = {}
        self.num_records = 0
        self.tps = 0

    def get_last_record(self, is_base):
        if is_base:
            if not self.base_records:
                return None
            return self.base_records[next(reversed(self.base_records))]
        if not self.tail_records:
            return None
        return self.tail_records[next(reversed(self.tail_records))]

    def get_record(self, is_base, rid):
        return self.base_records.get(rid) if is_base else self.tail_records.get(rid)

    def add_record(self, is_base, record):
        if is_base:
            self.base_records[record.rid] = record
            self.num_records += 1
        else:
            self.tail_records[record.rid] = record

    def add_page(self, is_base, column_number):
        if is_base:
            self.base_pages[column_number].append(Page())
        else:
            self.tail_pages[column_number].append(Page())