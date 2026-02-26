import threading
from lstore import page
from lstore.index import Index
from dataclasses import dataclass
from typing import List

from lstore.page import Page, PAGE_SIZE, COLUMN_ENTRY_SIZE

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
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)

        # Merge controls
        self.merge_threshold = 1000  # you can raise to 5000 if you want fewer merges
        self.page_range_directory = {}
        self._update_count = 0
        self._merge_lock = threading.Lock()

        # ✅ new: prevent overlapping background merges
        self._merge_running = False
        self._merge_thread = None

    def __getstate__(self):
        """Exclude unpicklable threading.Lock from serialization."""
        state = self.__dict__.copy()
        del state['_merge_lock']
        return state

    def __setstate__(self, state):
        """Restore threading.Lock on deserialization."""
        self.__dict__.update(state)
        self._merge_lock = threading.Lock()

    def get_primary_key(self, rid):
        page_directory_entry = self.page_directory[rid]
        page_range_number = page_directory_entry.page_range_number
        is_base = page_directory_entry.is_base
        page_range = self.page_range_directory[page_range_number]

        record = page_range.get_record(is_base, rid)
        primary_key = record.columns[self.key]
        return primary_key

    def add_page_range(self, page_range_number):
        if page_range_number in self.page_range_directory:
            return False
        self.page_range_directory[page_range_number] = PageRange(self, page_range_number)
        return True

    def add_record(self, page_range_number, is_base, *all_columns, record):
        page_range = self.page_range_directory[page_range_number]
        last_record = page_range.get_last_record(is_base)
        last_record_info = None

        if last_record is None:
            last_record_info = PageDirectoryEntry(
                page_range_number, is_base,
                [PageCoord(0, 0) for _ in range(self.num_columns + 3)]
            )
        else:
            last_record_rid = last_record.rid
            last_record_info = self.page_directory[last_record_rid]

        if is_base:
            pages = page_range.base_pages
        else:
            pages = page_range.tail_pages

        new_record_data_locations = [None] * (self.num_columns + 3)

        for column_number in range(self.num_columns + 3):
            if all_columns[column_number] is None:
                new_record_data_locations[column_number] = None
                continue

            last_page = pages[column_number][-1]
            last_record_page_number = len(pages[column_number]) - 1
            last_record_offset = last_page.current_offset

            if last_page.has_capacity():
                new_page_coord = PageCoord(last_record_page_number, last_record_offset)
            else:
                page_range.add_page(is_base, column_number)
                new_page_number = last_record_page_number + 1
                new_page_coord = PageCoord(new_page_number, 0)

            new_record_data_locations[column_number] = new_page_coord
            page_to_write = pages[column_number][new_page_coord.page_number]
            page_to_write.write(all_columns[column_number])

        new_page_directory_entry = PageDirectoryEntry(page_range_number, is_base, new_record_data_locations)
        self.page_directory[record.rid] = new_page_directory_entry
        page_range.add_record(is_base, record)
        return

    def construct_full_record(self, rid, relative_version=0):
        base_page_directory_entry = self.page_directory[rid]
        base_page_range_number = base_page_directory_entry.page_range_number
        base_data_locations = base_page_directory_entry.data_locations
        base_page_range = self.page_range_directory[base_page_range_number]

        base_rid_page_number = base_data_locations[RID_COLUMN].page_number
        base_rid_offset = base_data_locations[RID_COLUMN].offset
        base_rid_page = base_page_range.base_pages[RID_COLUMN][base_rid_page_number]
        base_rid = base_rid_page.read(base_rid_offset // page.COLUMN_ENTRY_SIZE)

        base_schema_page_number = base_data_locations[SCHEMA_ENCODING_COLUMN].page_number
        base_schema_offset = base_data_locations[SCHEMA_ENCODING_COLUMN].offset
        base_schema_page = base_page_range.base_pages[SCHEMA_ENCODING_COLUMN][base_schema_page_number]
        base_schema = format(base_schema_page.read(base_schema_offset // page.COLUMN_ENTRY_SIZE), f"0{self.num_columns}b")

        if base_schema == '0' * self.num_columns:
            return base_page_range.get_record(is_base=True, rid=rid).columns

        indirection_rid_page_number = base_data_locations[INDIRECTION_COLUMN].page_number
        indirection_rid_offset = base_data_locations[INDIRECTION_COLUMN].offset
        indirection_rid_page = base_page_range.base_pages[INDIRECTION_COLUMN][indirection_rid_page_number]
        indirection_rid = indirection_rid_page.read(indirection_rid_offset // page.COLUMN_ENTRY_SIZE)

        columns = [None] * self.num_columns
        version_num = 0

        while (indirection_rid != base_rid and indirection_rid is not None):
            # TPS optimization: stop following merged tail records (only for latest version)
            if relative_version == 0 and base_page_range.tps > 0 and indirection_rid <= base_page_range.tps:
                break

            current_page_directory_entry = self.page_directory[indirection_rid]
            current_page_range_number = current_page_directory_entry.page_range_number
            current_is_base = current_page_directory_entry.is_base
            current_data_locations = current_page_directory_entry.data_locations

            current_page_range = self.page_range_directory[current_page_range_number]
            current_record = current_page_range.get_record(is_base=current_is_base, rid=indirection_rid)
            current_columns = current_record.columns

            indirection_rid_page_number = current_data_locations[INDIRECTION_COLUMN].page_number
            indirection_rid_offset = current_data_locations[INDIRECTION_COLUMN].offset

            pages = current_page_range.base_pages if current_is_base else current_page_range.tail_pages
            indirection_rid_page = pages[INDIRECTION_COLUMN][indirection_rid_page_number]
            indirection_rid = indirection_rid_page.read(indirection_rid_offset // page.COLUMN_ENTRY_SIZE)

            if version_num >= relative_version:
                columns = [x if x is not None else y for x, y in zip(columns, current_columns)]
            version_num += 1

        # Fill remaining None columns from base.
        if base_page_range.tps > 0 and relative_version == 0:
            base_columns = []
            for col in range(self.num_columns):
                data_loc = base_data_locations[col + 3]
                if data_loc is not None:
                    bp = base_page_range.base_pages[col + 3][data_loc.page_number]
                    val = bp.read(data_loc.offset // page.COLUMN_ENTRY_SIZE)
                    base_columns.append(val)
                else:
                    base_columns.append(None)
        else:
            base_record = base_page_range.get_record(is_base=True, rid=base_rid)
            base_columns = base_record.columns

        columns = [x if x is not None else y for x, y in zip(columns, base_columns)]
        return columns

    # ✅ OPTIMIZED: do NOT build full record for latest version
    def get_column_value(self, rid, column_number, relative_version=0):
        # For older versions, keep original behavior (safe)
        if relative_version != 0:
            full_record_columns = self.construct_full_record(rid, relative_version)
            return full_record_columns[column_number]

        base_entry = self.page_directory[rid]
        pr = self.page_range_directory[base_entry.page_range_number]
        data_locs = base_entry.data_locations

        # Read base schema as int
        schema_loc = data_locs[SCHEMA_ENCODING_COLUMN]
        schema_page = pr.base_pages[SCHEMA_ENCODING_COLUMN][schema_loc.page_number]
        schema_int = schema_page.read(schema_loc.offset // COLUMN_ENTRY_SIZE)

        bit_mask = 1 << (self.num_columns - 1 - column_number)
        col_updated = (schema_int & bit_mask) != 0

        # Fast path: never updated -> read base pages directly
        if not col_updated:
            loc = data_locs[column_number + 3]
            if loc is None:
                return None
            bp = pr.base_pages[column_number + 3][loc.page_number]
            return bp.read(loc.offset // COLUMN_ENTRY_SIZE)

        # Follow indirection chain but only for this column
        ind_loc = data_locs[INDIRECTION_COLUMN]
        ind_page = pr.base_pages[INDIRECTION_COLUMN][ind_loc.page_number]
        ind_rid = ind_page.read(ind_loc.offset // COLUMN_ENTRY_SIZE)

        # Read base RID to stop
        rid_loc = data_locs[RID_COLUMN]
        rid_page = pr.base_pages[RID_COLUMN][rid_loc.page_number]
        base_rid = rid_page.read(rid_loc.offset // COLUMN_ENTRY_SIZE)

        while ind_rid is not None and ind_rid != base_rid:
            if pr.tps > 0 and ind_rid <= pr.tps:
                break

            entry = self.page_directory.get(ind_rid)
            if entry is None:
                break

            pr2 = self.page_range_directory[entry.page_range_number]
            rec = pr2.get_record(is_base=entry.is_base, rid=ind_rid)
            if rec is not None:
                v = rec.columns[column_number]
                if v is not None:
                    return v

            ind2 = entry.data_locations[INDIRECTION_COLUMN]
            pages = pr2.base_pages if entry.is_base else pr2.tail_pages
            ind_page2 = pages[INDIRECTION_COLUMN][ind2.page_number]
            ind_rid = ind_page2.read(ind2.offset // COLUMN_ENTRY_SIZE)

        # Fallback: base
        loc = data_locs[column_number + 3]
        if loc is None:
            return None
        bp = pr.base_pages[column_number + 3][loc.page_number]
        return bp.read(loc.offset // COLUMN_ENTRY_SIZE)

    # ✅ OPTIMIZED: real background merge (no join)
    def trigger_merge_check(self):
        self._update_count += 1
        if self._update_count < self.merge_threshold:
            return

        self._update_count = 0

        # Don't overlap merges
        if self._merge_running:
            return

        self._merge_running = True

        def _bg_merge_and_swap():
            try:
                results = self._merge()
                for pr_num, copied_pages, max_tail_rid in results:
                    page_range = self.page_range_directory[pr_num]
                    with self._merge_lock:
                        page_range.base_pages = copied_pages
                        page_range.tps = max_tail_rid

                    if Page._bufferpool is not None:
                        for col_pages in copied_pages:
                            for p in col_pages:
                                Page._bufferpool.access(p)
                                Page._bufferpool.mark_dirty(p.page_id)
            finally:
                self._merge_running = False

        self._merge_thread = threading.Thread(target=_bg_merge_and_swap, daemon=True)
        self._merge_thread.start()

    def _merge(self):
        results = []
        for pr_num in list(self.page_range_directory.keys()):
            result = self._merge_page_range(pr_num)
            if result is not None:
                results.append(result)
        return results

    def _merge_page_range(self, pr_num):
        page_range = self.page_range_directory.get(pr_num)
        if page_range is None or not page_range.tail_records:
            return None

        max_tail_rid = max(page_range.tail_records.keys())
        if max_tail_rid <= page_range.tps:
            return None

        # Copy base pages (contention-free)
        copied_pages = []
        for col in range(self.num_columns + 3):
            col_pages = []
            for orig_page in page_range.base_pages[col]:
                new_page = Page()
                new_page.num_records = orig_page.num_records
                new_page.current_offset = orig_page.current_offset
                orig_page._ensure_loaded()
                new_page.data = bytearray(orig_page.data)
                col_pages.append(new_page)
            copied_pages.append(col_pages)

        base_rids = [
            rid for rid, entry in self.page_directory.items()
            if entry.is_base and entry.page_range_number == pr_num
        ]

        for base_rid in base_rids:
            entry = self.page_directory[base_rid]
            data_locs = entry.data_locations

            latest_columns = self.construct_full_record(base_rid)

            for col in range(self.num_columns):
                data_loc = data_locs[col + 3]
                if data_loc is not None and latest_columns[col] is not None:
                    cp = copied_pages[col + 3][data_loc.page_number]
                    offset = data_loc.offset
                    val_bytes = latest_columns[col].to_bytes(8, byteorder='little')
                    cp.data[offset:offset + COLUMN_ENTRY_SIZE] = val_bytes

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
        self.tps = 0  # Tail-Page Sequence Number

    def get_last_record(self, is_base):
        if is_base:
            if len(self.base_records) == 0:
                return None
            return self.base_records[next(reversed(self.base_records))]
        else:
            if len(self.tail_records) == 0:
                return None
            return self.tail_records[next(reversed(self.tail_records))]

    def get_record(self, is_base, rid):
        if is_base:
            return self.base_records.get(rid, None)
        else:
            return self.tail_records.get(rid, None)

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