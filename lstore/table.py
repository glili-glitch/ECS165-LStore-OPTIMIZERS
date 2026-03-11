import threading
from dataclasses import dataclass
from typing import List
from lstore.index import Index
from lstore.page import Page

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
        self.page_range_directory = {}

        self._next_rid = 1
        self._rid_lock = threading.Lock()
        self.insert_lock = threading.Lock()
        self.record_lock = threading.Lock()

        self.index = Index(self)

        self.merge_threshold = 1000
        self._update_count = 0
        self._merge_lock = threading.Lock()

        self.lock_manager = None
        self.directory_lock = threading.Lock()

    def allocate_rid(self):
        with self._rid_lock:
            rid = self._next_rid
            self._next_rid += 1
            return rid

    def get_primary_key(self, rid):
        with self.directory_lock:
            entry = self.page_directory.get(rid)

        if not entry:
            return None

        pr = self.page_range_directory.get(entry.page_range_number)
        if not pr:
            return None

        record = pr.get_record(entry.is_base, rid)
        if not record:
            return None

        return record.columns[self.key]

    def add_page_range(self, page_range_number):
        with self.directory_lock:
            if page_range_number in self.page_range_directory:
                return False
            self.page_range_directory[page_range_number] = PageRange(self, page_range_number)
            return True

    def add_record(self, page_range_number, is_base, *all_columns, record):
        with self.record_lock:
            with self.directory_lock:
                pr = self.page_range_directory[page_range_number]

            pages = pr.base_pages if is_base else pr.tail_pages
            new_locs = [None] * (self.num_columns + 3)

            for i in range(self.num_columns + 3):
                if all_columns[i] is None:
                    continue

                last_page = pages[i][-1]
                if not last_page.has_capacity():
                    pr.add_page(is_base, i)
                    last_page = pages[i][-1]

                new_locs[i] = PageCoord(len(pages[i]) - 1, last_page.current_offset)
                last_page.write(all_columns[i])

            with self.directory_lock:
                self.page_directory[record.rid] = PageDirectoryEntry(
                    page_range_number=page_range_number,
                    is_base=is_base,
                    data_locations=new_locs
                )

            pr.add_record(is_base, record)

    def construct_full_record(self, rid, relative_version=0):
        with self.directory_lock:
            base_entry = self.page_directory.get(rid)

        if not base_entry or not base_entry.is_base:
            return None

        if relative_version > 0:
            return None

        def _read(entry, col_idx):
            loc = entry.data_locations[col_idx]
            if loc is None:
                return None

            pr = self.page_range_directory.get(entry.page_range_number)
            if pr is None:
                return None

            pages = pr.base_pages if entry.is_base else pr.tail_pages

            if col_idx >= len(pages):
                return None
            if loc.page_number >= len(pages[col_idx]):
                return None

            return pages[col_idx][loc.page_number].read(loc.offset // 8)

        base_values = [_read(base_entry, i + 3) for i in range(self.num_columns)]

        latest_tail_rid = _read(base_entry, INDIRECTION_COLUMN)

        if latest_tail_rid in (None, 0, rid):
            return base_values

        tail_chain = []
        visited = set()
        curr_rid = latest_tail_rid

        while curr_rid not in (None, 0, rid) and curr_rid not in visited:
            visited.add(curr_rid)

            with self.directory_lock:
                tail_entry = self.page_directory.get(curr_rid)

            if tail_entry is None or tail_entry.is_base:
                break

            tail_chain.append(tail_entry)

            next_rid = _read(tail_entry, INDIRECTION_COLUMN)
            if next_rid == curr_rid:
                break
            curr_rid = next_rid

        versions = [base_values[:]]
        current = base_values[:]

        skip = abs(relative_version)

        if skip >= len(tail_chain):
            usable_chain = []
        else:
            usable_chain = tail_chain[skip:]


        for tail_entry in reversed(usable_chain):
            for i in range(self.num_columns):
                val = _read(tail_entry, i + 3)
                if val is not None:
                    current[i] = val
            versions.append(current[:])

        latest_index = len(versions) - 1
        target_index = latest_index + relative_version

        if target_index < 0:
            target_index = 0

        return versions[target_index]

    def get_column_value(self, rid, column_number, relative_version=0):
        full_record = self.construct_full_record(rid, relative_version)
        if full_record is None:
            return None
        return full_record[column_number]

    def trigger_merge_check(self):
        return

    def _bg_merge_task(self):
        results = self._merge()
        for pr_num, copied_pages, max_tail_rid in results:
            pr = self.page_range_directory.get(pr_num)
            if pr:
                with self._merge_lock:
                    if max_tail_rid > pr.tps:
                        pr.base_pages = copied_pages
                        pr.tps = max_tail_rid

    def _merge(self):
        results = []
        pr_keys = list(self.page_range_directory.keys())
        for pr_num in pr_keys:
            result = self._merge_page_range(pr_num)
            if result is not None:
                results.append(result)
        return results

    def _merge_page_range(self, pr_num):
        pr = self.page_range_directory.get(pr_num)
        if pr is None or not pr.tail_records:
            return None

        with self.directory_lock:
            current_max_rid = max(pr.tail_records.keys())
            if current_max_rid <= pr.tps:
                return None

            base_rids = [
                rid for rid, entry in self.page_directory.items()
                if entry.is_base and entry.page_range_number == pr_num
            ]

        copied_pages = []
        for col in range(self.num_columns + 3):
            copied_col = []
            for original_page in pr.base_pages[col]:
                new_page = Page()
                new_page.num_records = original_page.num_records
                new_page.current_offset = original_page.current_offset
                original_page._ensure_loaded()
                new_page.data = bytearray(original_page.data)
                copied_col.append(new_page)
            copied_pages.append(copied_col)

        for base_rid in base_rids:
            latest_values = self.construct_full_record(base_rid, 0)
            if latest_values is None:
                continue

            with self.directory_lock:
                entry = self.page_directory.get(base_rid)
                if entry is None:
                    continue
                locations = entry.data_locations

            for col in range(self.num_columns):
                loc = locations[col + 3]
                if loc is None:
                    continue

                value = latest_values[col]
                if value is None:
                    continue

                copied_pages[col + 3][loc.page_number].data[loc.offset:loc.offset + 8] = int(value).to_bytes(
                    8, byteorder="little", signed=True
                )

        return pr_num, copied_pages, current_max_rid

    def rollback_record(self, rid, old_indirection, old_schema):
        with self.directory_lock:
            entry = self.page_directory.get(rid)
            if not entry:
                return
            pr = self.page_range_directory[entry.page_range_number]

            ind_loc = entry.data_locations[INDIRECTION_COLUMN]
            ind_page = pr.base_pages[INDIRECTION_COLUMN][ind_loc.page_number]
            ind_page.write(old_indirection, offset=ind_loc.offset)

            sch_loc = entry.data_locations[SCHEMA_ENCODING_COLUMN]
            sch_page = pr.base_pages[SCHEMA_ENCODING_COLUMN][sch_loc.page_number]
            sch_page.write(old_schema, offset=sch_loc.offset)

    def set_indirection(self, rid, val):
        with self.directory_lock:
            e = self.page_directory.get(rid)
            pr = self.page_range_directory[e.page_range_number]
            l = e.data_locations[INDIRECTION_COLUMN]
            pr.base_pages[INDIRECTION_COLUMN][l.page_number].write(val, offset=l.offset)

    def set_schema(self, rid, val):
        with self.directory_lock:
            e = self.page_directory.get(rid)
            pr = self.page_range_directory[e.page_range_number]
            l = e.data_locations[SCHEMA_ENCODING_COLUMN]
            pr.base_pages[SCHEMA_ENCODING_COLUMN][l.page_number].write(val, offset=l.offset)

    def delete_record(self, rid, columns):
        with self.directory_lock:
            entry = self.page_directory.pop(rid, None)
            if entry:
                pr = self.page_range_directory[entry.page_range_number]
                if rid in pr.base_records:
                    del pr.base_records[rid]
                    pr.num_records -= 1
        for i, val in enumerate(columns):
            self.index.remove_from_index(i, val, rid)

    def get_indirection(self, rid):
        with self.directory_lock:
            entry = self.page_directory.get(rid)
        if not entry:
            return 0
        pr = self.page_range_directory[entry.page_range_number]
        loc = entry.data_locations[INDIRECTION_COLUMN]
        return pr.base_pages[INDIRECTION_COLUMN][loc.page_number].read(loc.offset // 8)

    def get_schema(self, rid):
        with self.directory_lock:
            entry = self.page_directory.get(rid)
        if not entry:
            return 0
        pr = self.page_range_directory[entry.page_range_number]
        loc = entry.data_locations[SCHEMA_ENCODING_COLUMN]
        return pr.base_pages[SCHEMA_ENCODING_COLUMN][loc.page_number].read(loc.offset // 8)


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
        records = self.base_records if is_base else self.tail_records
        if not records:
            return None
        return records[next(reversed(records))]

    def get_record(self, is_base, rid):
        records = self.base_records if is_base else self.tail_records
        return records.get(rid)

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