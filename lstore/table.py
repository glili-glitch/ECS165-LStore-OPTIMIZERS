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
    :param name: string
    :param num_columns: int
    :param key: int
    """

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
        self.lock_manager = None
        self.directory_lock = threading.Lock()

    def get_primary_key(self, rid):
        page_directory_entry = self.page_directory[rid]
        page_range_number = page_directory_entry.page_range_number
        is_base = page_directory_entry.is_base
        page_range = self.page_range_directory[page_range_number]

        record = page_range.get_record(is_base, rid)
        return record.columns[self.key]

    def add_page_range(self, page_range_number):
        with self.directory_lock:
            if page_range_number in self.page_range_directory:
                return False
            self.page_range_directory[page_range_number] = PageRange(self, page_range_number)
            return True

    def add_record(self, page_range_number, is_base, *all_columns, record):
        with self.directory_lock:
            page_range = self.page_range_directory[page_range_number]

        last_record = page_range.get_last_record(is_base)
        if last_record is None:
            last_record_info = PageDirectoryEntry(
                page_range_number,
                is_base,
                [PageCoord(0, 0) for _ in range(self.num_columns + 3)]
            )
        else:
            last_record_rid = last_record.rid
            with self.directory_lock:
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

        new_page_directory_entry = PageDirectoryEntry(
            page_range_number, is_base, new_record_data_locations
        )

        with self.directory_lock:
            self.page_directory[record.rid] = new_page_directory_entry

        page_range.add_record(is_base, record)

    def construct_full_record(self, rid, relative_version=0):
        with self.directory_lock:
            if rid not in self.page_directory:
                return None

        return [
            self.get_column_value(rid, col, relative_version)
            for col in range(self.num_columns)
        ]

    def get_column_value(self, rid, column_number, relative_version=0):
        with self.directory_lock:
            if rid not in self.page_directory:
                return None

            base_entry = self.page_directory[rid]
            if not base_entry.is_base:
                return None

            page_range_number = base_entry.page_range_number
            base_data_locations = base_entry.data_locations
            page_range = self.page_range_directory[page_range_number]

        def read_meta(entry, meta_col):
            if entry is None:
                return None
            loc = entry.data_locations[meta_col]
            if loc is None:
                return None
            pr = self.page_range_directory[entry.page_range_number]
            pages = pr.base_pages if entry.is_base else pr.tail_pages
            p = pages[meta_col][loc.page_number]
            return p.read(loc.offset // page.COLUMN_ENTRY_SIZE)

        def read_user_value(entry, col_num):
            if entry is None:
                return None
            loc = entry.data_locations[col_num + 3]
            if loc is None:
                return None
            pr = self.page_range_directory[entry.page_range_number]
            pages = pr.base_pages if entry.is_base else pr.tail_pages
            p = pages[col_num + 3][loc.page_number]
            return p.read(loc.offset // page.COLUMN_ENTRY_SIZE)

        base_rid = read_meta(base_entry, RID_COLUMN)
        base_schema = read_meta(base_entry, SCHEMA_ENCODING_COLUMN)

        # never updated
        if base_schema == 0:
            return read_user_value(base_entry, column_number)

        latest_tail_rid = read_meta(base_entry, INDIRECTION_COLUMN)
        if latest_tail_rid in (None, 0, base_rid):
            return read_user_value(base_entry, column_number)

        # Build tail chain: newest -> oldest
        tail_chain = []
        current_rid = latest_tail_rid

        while current_rid not in (None, 0, base_rid):
            with self.directory_lock:
                current_entry = self.page_directory.get(current_rid)

            if current_entry is None:
                break

            tail_chain.append(current_rid)
            next_rid = read_meta(current_entry, INDIRECTION_COLUMN)

            if next_rid == current_rid:
                break

            current_rid = next_rid

        if not tail_chain:
            return read_user_value(base_entry, column_number)

        # oldest tail is the original snapshot created on first update
        snapshot_rid = tail_chain[-1]
        with self.directory_lock:
            snapshot_entry = self.page_directory.get(snapshot_rid)

        current_value = read_user_value(snapshot_entry, column_number)
        if current_value is None:
            current_value = read_user_value(base_entry, column_number)

        # logical update tails in oldest -> newest order
        update_tail_rids = list(reversed(tail_chain[:-1]))

        if relative_version >= 0:
            updates_to_apply = len(update_tail_rids)
        else:
            steps_back = abs(relative_version)
            updates_to_apply = max(0, len(update_tail_rids) - steps_back)

        for tail_rid in update_tail_rids[:updates_to_apply]:
            with self.directory_lock:
                tail_entry = self.page_directory.get(tail_rid)

            if tail_entry is None:
                continue

            val = read_user_value(tail_entry, column_number)
            if val is not None:
                current_value = val

        return current_value

    def trigger_merge_check(self):
        self._update_count += 1
        if self._update_count >= self.merge_threshold:
            self._update_count = 0
            merge_results = []

            def _bg_merge():
                for pr_num in list(self.page_range_directory.keys()):
                    result = self._merge_page_range(pr_num)
                    if result is not None:
                        merge_results.append(result)

            merge_thread = threading.Thread(target=_bg_merge, daemon=True)
            merge_thread.start()
            merge_thread.join()

            for pr_num, copied_pages, max_tail_rid in merge_results:
                page_range = self.page_range_directory[pr_num]
                with self._merge_lock:
                    page_range.base_pages = copied_pages
                    page_range.tps = max_tail_rid

                if Page._bufferpool is not None:
                    for col_pages in copied_pages:
                        for p in col_pages:
                            Page._bufferpool.access(p)
                            Page._bufferpool.mark_dirty(p.page_id)

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
            latest_columns = self.construct_full_record(base_rid, 0)

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
        self.tps = 0

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