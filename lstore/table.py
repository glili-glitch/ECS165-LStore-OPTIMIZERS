import threading
from lstore import page
from lstore.index import Index
from time import time
from dataclasses import dataclass
from typing import List

from lstore.page import Page, PAGE_SIZE, COLUMN_ENTRY_SIZE

RID_COLUMN = 0
INDIRECTION_COLUMN = 1
# TIMESTAMP_COLUMN = 2
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
        self.merge_threshold = 1000  # Number of updates before triggering merge
        self.page_range_directory = {}
        self._update_count = 0
        self._merge_lock = threading.Lock()

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
        last_record_data_locations = None

        if last_record is None:
            last_record_info = PageDirectoryEntry(page_range_number, is_base, [PageCoord(0, 0) for _ in range(self.num_columns + 3)])
        else:
            last_record_rid = last_record.rid
            last_record_info = self.page_directory[last_record_rid]

        last_record_data_locations = last_record_info.data_locations
        
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
            last_record_page_number = pages[column_number].__len__() - 1
            last_record_offset = pages[column_number][-1].current_offset

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
            record = base_page_range.get_record(is_base=True, rid=rid).columns
            return base_page_range.get_record(is_base=True, rid=rid).columns

        indirection_rid_page_number = base_data_locations[INDIRECTION_COLUMN].page_number
        indirection_rid_offset = base_data_locations[INDIRECTION_COLUMN].offset
        indirection_rid_page = base_page_range.base_pages[INDIRECTION_COLUMN][indirection_rid_page_number]
        indirection_rid = indirection_rid_page.read(indirection_rid_offset // page.COLUMN_ENTRY_SIZE)

        columns = [None] * self.num_columns
        version_num = 0

        while (indirection_rid != base_rid and indirection_rid != None):
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
                new_columns = [x if x is not None else y for x, y in zip(columns, current_columns)]
                columns = new_columns
            version_num += 1
        
        # Fill remaining None columns from base.
        # If TPS > 0, the merge has updated the base PAGE data with latest values,
        # so read directly from pages for accuracy.
        if base_page_range.tps > 0 and relative_version == 0:
            # Read from merged base pages
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
            # No merge or requesting older version — use Record.columns
            base_record = base_page_range.get_record(is_base=True, rid=base_rid)
            base_columns = base_record.columns

        new_columns = [x if x is not None else y for x, y in zip(columns, base_columns)]
        columns = new_columns
        
        return columns 

    def get_column_value(self, rid, column_number, relative_version=0):
        full_record_columns = self.construct_full_record(rid, relative_version)
        return full_record_columns[column_number]

    def trigger_merge_check(self):
        """Called after each update. Triggers background merge when threshold is reached."""
        self._update_count += 1
        if self._update_count >= self.merge_threshold:
            self._update_count = 0
            # Background thread does the heavy computation (copying + merging)
            merge_results = []
            def _bg_merge():
                for pr_num in list(self.page_range_directory.keys()):
                    result = self._merge_page_range(pr_num)
                    if result is not None:
                        merge_results.append(result)

            merge_thread = threading.Thread(target=_bg_merge, daemon=True)
            merge_thread.start()
            merge_thread.join()

            # Page directory modification happens on the main thread (foreground)
            # per PDF spec: "The modification of the page directory still needs to
            # happen on the main thread (foreground)"
            for pr_num, copied_pages, max_tail_rid in merge_results:
                page_range = self.page_range_directory[pr_num]
                with self._merge_lock:
                    page_range.base_pages = copied_pages
                    page_range.tps = max_tail_rid

                # Register merged pages with bufferpool so they persist on close()
                if Page._bufferpool is not None:
                    for col_pages in copied_pages:
                        for p in col_pages:
                            Page._bufferpool.access(p)
                            Page._bufferpool.mark_dirty(p.page_id)

    def _merge(self):
        """Background merge: consolidate tail records into base pages."""
        results = []
        for pr_num in list(self.page_range_directory.keys()):
            result = self._merge_page_range(pr_num)
            if result is not None:
                results.append(result)
        return results

    def _merge_page_range(self, pr_num):
        """
        Merge tail records into base pages for a single page range (contention-free).
        Returns (pr_num, copied_pages, max_tail_rid) or None if no merge needed.
        The actual page directory swap is done by the caller on the main thread.
        """
        page_range = self.page_range_directory.get(pr_num)
        if page_range is None or not page_range.tail_records:
            return None

        max_tail_rid = max(page_range.tail_records.keys())
        if max_tail_rid <= page_range.tps:
            return None  # Already up-to-date

        # Step 1: Create COPIES of base pages (outside bufferpool for contention-free merge)
        copied_pages = []
        for col in range(self.num_columns + 3):
            col_pages = []
            for orig_page in page_range.base_pages[col]:
                new_page = Page()  # New page with unique ID
                new_page.num_records = orig_page.num_records
                new_page.current_offset = orig_page.current_offset
                orig_page._ensure_loaded()
                new_page.data = bytearray(orig_page.data)
                col_pages.append(new_page)
            copied_pages.append(col_pages)

        # Step 2: For each base record, write latest values into the copied pages
        base_rids = [rid for rid, entry in self.page_directory.items()
                     if entry.is_base and entry.page_range_number == pr_num]

        for base_rid in base_rids:
            entry = self.page_directory[base_rid]
            data_locs = entry.data_locations

            # Get the latest column values by following the full tail chain
            latest_columns = self.construct_full_record(base_rid)

            # Write merged values into copied base pages (user columns only)
            for col in range(self.num_columns):
                data_loc = data_locs[col + 3]
                if data_loc is not None and latest_columns[col] is not None:
                    cp = copied_pages[col + 3][data_loc.page_number]
                    offset = data_loc.offset
                    val_bytes = latest_columns[col].to_bytes(8, byteorder='little')
                    cp.data[offset:offset + COLUMN_ENTRY_SIZE] = val_bytes

        # Return the result — the swap will be done on the main thread
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
        self.tps = 0  # Tail-Page Sequence Number (RID of last merged tail record)

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

 
