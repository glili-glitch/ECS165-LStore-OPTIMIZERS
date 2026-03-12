import threading
from dataclasses import dataclass
from typing import List
from lstore.lock_manager import LockManager
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
    is_deleted: bool = False


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
        self._next_base_rid = 1
        self._next_tail_rid = 10_000_000_000
        self._rid_lock = threading.Lock()
        self.insert_lock = threading.Lock()
        self.record_lock = threading.Lock()
        self.index = Index(self)
        self.merge_threshold = 1000
        self._update_count = 0
        self._merge_lock = threading.Lock()
        self.lock_manager = LockManager()
        self.directory_lock = threading.Lock()

    

    def allocate_base_rid(self):
        with self._rid_lock:
            rid = self._next_base_rid
            self._next_base_rid += 1
            return rid

    def allocate_tail_rid(self):
        with self._rid_lock:
            rid = self._next_tail_rid
            self._next_tail_rid += 1
            return rid

    def _read_page_value(self, entry, col_idx):
        if entry is None:
            return None
        if col_idx < 0 or col_idx >= len(entry.data_locations):
            return None
        loc = entry.data_locations[col_idx]
        if loc is None:
            return None
        pr = self.page_range_directory.get(entry.page_range_number)
        if pr is None:
            return None
        pages = pr.base_pages if entry.is_base else pr.tail_pages
        if col_idx >= len(pages):
            return None
        if loc.page_number < 0 or loc.page_number >= len(pages[col_idx]):
            return None
        return pages[col_idx][loc.page_number].read(loc.offset // 8)

    def get_primary_key(self, rid):
        full_record = self.construct_full_record(rid, 0)
        if full_record is None:
            return None
        if self.key < 0 or self.key >= len(full_record):
            return None
        return full_record[self.key]

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
                if i >= len(all_columns):
                    break
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
                    data_locations=new_locs,
                    is_deleted=False
                )

            pr.add_record(is_base, record)

    def delete_tail_record(self, rid):
        with self.directory_lock:
            entry = self.page_directory.pop(rid, None)

        if not entry:
            return

        pr = self.page_range_directory.get(entry.page_range_number)
        if pr is None:
            return

        if not entry.is_base and rid in pr.tail_records:
            del pr.tail_records[rid]

    def construct_full_record(self, rid, relative_version=0):
        with self.directory_lock:
            base_entry = self.page_directory.get(rid)

        if base_entry is None or not base_entry.is_base or base_entry.is_deleted:
            return None

        if relative_version > 0:
            return None

        pr = self.page_range_directory.get(base_entry.page_range_number)
        if pr is None:
            return None

        base_record = pr.get_record(True, rid)
        if base_record is None or base_record.columns is None:
            return None

        if len(base_record.columns) != self.num_columns:
            return None

        base_values = list(base_record.columns)

        latest_tail_rid = self._read_page_value(base_entry, INDIRECTION_COLUMN)
        if latest_tail_rid in (None, 0, rid):
            return base_values[:]

        tail_chain = []
        visited = set()
        curr_rid = latest_tail_rid

        while curr_rid not in (None, 0, rid) and curr_rid not in visited:
            visited.add(curr_rid)

            with self.directory_lock:
                tail_entry = self.page_directory.get(curr_rid)

            if tail_entry is None or tail_entry.is_base or tail_entry.is_deleted:
                break

            tail_record = pr.get_record(False, curr_rid)
            if tail_record is None or tail_record.columns is None:
                break

            if len(tail_record.columns) != self.num_columns:
                break

            tail_chain.append(tail_record)

            next_rid = self._read_page_value(tail_entry, INDIRECTION_COLUMN)
            if next_rid == curr_rid:
                break
            curr_rid = next_rid

        if not tail_chain:
            return base_values[:] if relative_version == 0 else None

        versions = [base_values[:]]
        current = base_values[:]

        for tail_record in reversed(tail_chain):
            new_version = current[:]
            changed = False

            for i in range(self.num_columns):
                if tail_record.columns[i] is not None:
                    new_version[i] = tail_record.columns[i]
                    changed = True

            if changed:
                if new_version != versions[-1]:
                    versions.append(new_version[:])
                current = new_version[:]

        if relative_version == 0:
            return versions[-1][:]

        idx = len(versions) - 1 + relative_version
        if idx < 0:
            idx = 0

        if idx >= len(versions):
            return None

        return versions[idx][:]

    def get_column_value(self, rid, column_number, relative_version=0):
        full_record = self.construct_full_record(rid, relative_version)
        if full_record is None:
            return None
        if column_number < 0 or column_number >= len(full_record):
            return None
        return full_record[column_number]

    def trigger_merge_check(self):
        return

    def rollback_record(self, rid, old_indirection, old_schema):
        with self.directory_lock:
            entry = self.page_directory.get(rid)
            if not entry or entry.is_deleted:
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
            if not e or e.is_deleted:
                return
            pr = self.page_range_directory[e.page_range_number]
            l = e.data_locations[INDIRECTION_COLUMN]
            pr.base_pages[INDIRECTION_COLUMN][l.page_number].write(val, offset=l.offset)

    def set_schema(self, rid, val):
        with self.directory_lock:
            e = self.page_directory.get(rid)
            if not e or e.is_deleted:
                return
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
                elif rid in pr.tail_records:
                    del pr.tail_records[rid]

        for i, val in enumerate(columns):
            self.index.remove_from_index(i, val, rid)

    def get_indirection(self, rid):
        with self.directory_lock:
            entry = self.page_directory.get(rid)

        if not entry or entry.is_deleted or not entry.is_base:
            return 0

        pr = self.page_range_directory[entry.page_range_number]
        loc = entry.data_locations[INDIRECTION_COLUMN]
        return pr.base_pages[INDIRECTION_COLUMN][loc.page_number].read(loc.offset // 8)

    def get_schema(self, rid):
        with self.directory_lock:
            entry = self.page_directory.get(rid)

        if not entry or entry.is_deleted or not entry.is_base:
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
