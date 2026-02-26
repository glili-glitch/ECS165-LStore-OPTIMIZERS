import threading
from lstore import page
from lstore.index import Index
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import array
import functools

from lstore.page import Page, PAGE_SIZE, COLUMN_ENTRY_SIZE

RID_COLUMN = 0
INDIRECTION_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2

NUM_RECORDS_PER_RANGE = 1024


@dataclass
class PageCoord:
    __slots__ = ('page_number', 'offset')
    page_number: int
    offset: int


@dataclass
class PageDirectoryEntry:
    __slots__ = ('page_range_number', 'is_base', 'data_locations')
    page_range_number: int
    is_base: bool
    data_locations: List[PageCoord]


class Record:
    __slots__ = ('rid', 'key', 'columns')
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
        self.page_directory: Dict[int, PageDirectoryEntry] = {}
        self.index = Index(self)

        # Merge controls
        self.merge_threshold = 5000
        self.page_range_directory: Dict[int, 'PageRange'] = {}
        self._update_count = 0
        self._merge_lock = threading.Lock()
        
        # OPTIMIZATION: Cache frequently used values
        self._total_columns = num_columns + 3
        self._column_range = range(self._total_columns)
        self._data_column_offset = 3
        
        # OPTIMIZATION: Pre-compute bit masks for schema encoding
        self._bit_masks = [1 << (num_columns - 1 - i) for i in range(num_columns)]
        
        # OPTIMIZATION: Cache for column values (LRU-like)
        self._value_cache = {}
        self._cache_size = 1000
        self._cache_hits = 0
        self._cache_misses = 0

    def __getstate__(self):
        """Exclude unpicklable threading.Lock from serialization."""
        state = self.__dict__.copy()
        del state['_merge_lock']
        del state['_value_cache']  # Don't pickle cache
        return state

    def __setstate__(self, state):
        """Restore threading.Lock on deserialization."""
        self.__dict__.update(state)
        self._merge_lock = threading.Lock()
        self._value_cache = {}  # Recreate cache

    def _cache_get(self, key):
        """Get value from cache with LRU behavior"""
        if key in self._value_cache:
            self._cache_hits += 1
            return self._value_cache[key]
        self._cache_misses += 1
        return None
    
    def _cache_put(self, key, value):
        """Put value in cache with size limit"""
        if len(self._value_cache) >= self._cache_size:
            # Simple FIFO eviction (could be improved to LRU)
            self._value_cache.pop(next(iter(self._value_cache)))
        self._value_cache[key] = value

    def get_primary_key(self, rid):
        """Get primary key for a record"""
        # OPTIMIZATION: Try cache first
        cache_key = (rid, 'pk')
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
            
        page_directory_entry = self.page_directory.get(rid)
        if not page_directory_entry:
            return None
            
        page_range_number = page_directory_entry.page_range_number
        page_range = self.page_range_directory.get(page_range_number)
        if not page_range:
            return None

        record = page_range.get_record(page_directory_entry.is_base, rid)
        if not record:
            return None
            
        primary_key = record.columns[self.key]
        self._cache_put(cache_key, primary_key)
        return primary_key

    def add_page_range(self, page_range_number):
        """Add a new page range"""
        if page_range_number in self.page_range_directory:
            return False
            
        self.page_range_directory[page_range_number] = PageRange(self, page_range_number)
        return True

    def add_record(self, page_range_number, is_base, *all_columns, record):
        """Add a record to a page range"""
        page_range = self.page_range_directory.get(page_range_number)
        if not page_range:
            return False

        # OPTIMIZATION: Get last record info once
        last_record = page_range.get_last_record(is_base)
        if last_record is None:
            # First record in range
            new_record_data_locations = [PageCoord(0, 0) for _ in range(self._total_columns)]
        else:
            last_record_info = self.page_directory.get(last_record.rid)
            if not last_record_info:
                new_record_data_locations = [None] * self._total_columns
            else:
                # OPTIMIZATION: Copy last locations as starting point
                new_record_data_locations = last_record_info.data_locations.copy()

        pages = page_range.base_pages if is_base else page_range.tail_pages

        # OPTIMIZATION: Batch process columns
        for column_number in self._column_range:
            value = all_columns[column_number]
            if value is None:
                new_record_data_locations[column_number] = None
                continue

            # Get or create page for this column
            if not pages[column_number]:
                pages[column_number].append(Page())
                
            last_page = pages[column_number][-1]
            
            if last_page.has_capacity():
                page_num = len(pages[column_number]) - 1
                offset = last_page.current_offset
            else:
                # Add new page
                pages[column_number].append(Page())
                page_num = len(pages[column_number]) - 1
                offset = 0

            new_record_data_locations[column_number] = PageCoord(page_num, offset)
            
            # Write value
            target_page = pages[column_number][page_num]
            target_page.write(value)

        # Create directory entry
        self.page_directory[record.rid] = PageDirectoryEntry(
            page_range_number, is_base, new_record_data_locations
        )
        
        # Add to page range
        page_range.add_record(is_base, record)
        
        # OPTIMIZATION: Invalidate cache for this rid
        keys_to_remove = [k for k in self._value_cache if k[0] == record.rid]
        for k in keys_to_remove:
            self._value_cache.pop(k, None)
            
        return True

    def construct_full_record(self, rid, relative_version=0):
        """Construct a full record from base and tail records"""
        # OPTIMIZATION: Try cache first
        cache_key = (rid, relative_version, 'full')
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
            
        base_entry = self.page_directory.get(rid)
        if not base_entry:
            return None
            
        pr_num = base_entry.page_range_number
        base_locs = base_entry.data_locations
        pr = self.page_range_directory.get(pr_num)
        if not pr:
            return None

        # OPTIMIZATION: Pre-fetch frequently accessed pages
        rid_loc = base_locs[RID_COLUMN]
        if rid_loc is None:
            return None
            
        rid_page = pr.base_pages[RID_COLUMN][rid_loc.page_number]
        base_rid = rid_page.read(rid_loc.offset // COLUMN_ENTRY_SIZE)

        schema_loc = base_locs[SCHEMA_ENCODING_COLUMN]
        if schema_loc is None:
            return None
            
        schema_page = pr.base_pages[SCHEMA_ENCODING_COLUMN][schema_loc.page_number]
        schema_int = schema_page.read(schema_loc.offset // COLUMN_ENTRY_SIZE)

        # OPTIMIZATION: Quick path for no updates
        if schema_int == 0:
            result = pr.get_record(is_base=True, rid=rid).columns
            self._cache_put(cache_key, result)
            return result

        ind_loc = base_locs[INDIRECTION_COLUMN]
        if ind_loc is None:
            return None
            
        ind_page = pr.base_pages[INDIRECTION_COLUMN][ind_loc.page_number]
        ind_rid = ind_page.read(ind_loc.offset // COLUMN_ENTRY_SIZE)

        columns = [None] * self.num_columns
        version_num = 0
        visited = set()  # OPTIMIZATION: Detect cycles

        # OPTIMIZATION: Batch collect tail values
        tail_values = []
        tail_rids = []

        while ind_rid is not None and ind_rid != base_rid and ind_rid not in visited:
            visited.add(ind_rid)
            
            if relative_version == 0 and pr.tps > 0 and ind_rid <= pr.tps:
                break

            entry = self.page_directory.get(ind_rid)
            if not entry:
                break

            pr2 = self.page_range_directory.get(entry.page_range_number)
            if not pr2:
                break
                
            rec = pr2.get_record(is_base=entry.is_base, rid=ind_rid)
            if rec:
                tail_values.append((version_num, rec.columns))
                tail_rids.append(ind_rid)

            ind2 = entry.data_locations[INDIRECTION_COLUMN]
            if not ind2:
                break
                
            pages = pr2.base_pages if entry.is_base else pr2.tail_pages
            ind_page2 = pages[INDIRECTION_COLUMN][ind2.page_number]
            ind_rid = ind_page2.read(ind2.offset // COLUMN_ENTRY_SIZE)
            version_num += 1

        # OPTIMIZATION: Apply tail values in reverse order
        for version_num, tail_cols in reversed(tail_values):
            if version_num >= relative_version:
                for i in range(self.num_columns):
                    if tail_cols[i] is not None and columns[i] is None:
                        columns[i] = tail_cols[i]

        # Get base columns
        if pr.tps > 0 and relative_version == 0:
            base_cols = []
            for col in range(self.num_columns):
                loc = base_locs[col + self._data_column_offset]
                if loc is not None:
                    bp = pr.base_pages[col + self._data_column_offset][loc.page_number]
                    base_cols.append(bp.read(loc.offset // COLUMN_ENTRY_SIZE))
                else:
                    base_cols.append(None)
        else:
            base_rec = pr.get_record(is_base=True, rid=base_rid)
            base_cols = base_rec.columns if base_rec else [None] * self.num_columns

        # Merge base and tail columns
        result = [base_cols[i] if columns[i] is None else columns[i] for i in range(self.num_columns)]
        
        self._cache_put(cache_key, result)
        return result

    # Optimized for latest version: column-only scan
    def get_column_value(self, rid, column_number, relative_version=0):
        """Get a single column value efficiently"""
        # OPTIMIZATION: Try cache first
        cache_key = (rid, column_number, relative_version)
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        # old versions: keep safe full reconstruction
        if relative_version != 0:
            cols = self.construct_full_record(rid, relative_version)
            result = cols[column_number] if cols else None
            self._cache_put(cache_key, result)
            return result

        base_entry = self.page_directory.get(rid)
        if not base_entry:
            return None
            
        pr = self.page_range_directory.get(base_entry.page_range_number)
        if not pr:
            return None
            
        locs = base_entry.data_locations

        # Check schema to see if column was updated
        schema_loc = locs[SCHEMA_ENCODING_COLUMN]
        if not schema_loc:
            return None
            
        schema_page = pr.base_pages[SCHEMA_ENCODING_COLUMN][schema_loc.page_number]
        schema_int = schema_page.read(schema_loc.offset // COLUMN_ENTRY_SIZE)

        # OPTIMIZATION: Use pre-computed bit mask
        updated = (schema_int & self._bit_masks[column_number]) != 0

        if not updated:
            loc = locs[column_number + self._data_column_offset]
            if loc is None:
                return None
            bp = pr.base_pages[column_number + self._data_column_offset][loc.page_number]
            result = bp.read(loc.offset // COLUMN_ENTRY_SIZE)
            self._cache_put(cache_key, result)
            return result

        # Column was updated, need to follow indirection chain
        ind_loc = locs[INDIRECTION_COLUMN]
        if not ind_loc:
            return None
            
        ind_page = pr.base_pages[INDIRECTION_COLUMN][ind_loc.page_number]
        ind_rid = ind_page.read(ind_loc.offset // COLUMN_ENTRY_SIZE)

        rid_loc = locs[RID_COLUMN]
        if not rid_loc:
            return None
            
        rid_page = pr.base_pages[RID_COLUMN][rid_loc.page_number]
        base_rid = rid_page.read(rid_loc.offset // COLUMN_ENTRY_SIZE)

        visited = set()  # OPTIMIZATION: Detect cycles

        while ind_rid is not None and ind_rid != base_rid and ind_rid not in visited:
            visited.add(ind_rid)
            
            if pr.tps > 0 and ind_rid <= pr.tps:
                break

            entry = self.page_directory.get(ind_rid)
            if entry is None:
                break

            pr2 = self.page_range_directory.get(entry.page_range_number)
            if pr2 is None:
                break
                
            rec = pr2.get_record(is_base=entry.is_base, rid=ind_rid)
            if rec is not None:
                val = rec.columns[column_number]
                if val is not None:
                    self._cache_put(cache_key, val)
                    return val

            ind2 = entry.data_locations[INDIRECTION_COLUMN]
            if ind2 is None:
                break
                
            pages = pr2.base_pages if entry.is_base else pr2.tail_pages
            ind_page2 = pages[INDIRECTION_COLUMN][ind2.page_number]
            ind_rid = ind_page2.read(ind2.offset // COLUMN_ENTRY_SIZE)

        # Fall back to base value
        loc = locs[column_number + self._data_column_offset]
        if loc is None:
            return None
            
        bp = pr.base_pages[column_number + self._data_column_offset][loc.page_number]
        result = bp.read(loc.offset // COLUMN_ENTRY_SIZE)
        self._cache_put(cache_key, result)
        return result

    # Synchronous merge check 
    def trigger_merge_check(self):
        """Check if merge should be triggered"""
        self._update_count += 1
        if self._update_count < self.merge_threshold:
            return

        self._update_count = 0

        # OPTIMIZATION: Only merge if there are updates
        merge_results = []
        for pr_num, page_range in list(self.page_range_directory.items()):
            if page_range and page_range.tail_records:  # Only if there are tail records
                result = self._merge_page_range(pr_num)
                if result is not None:
                    merge_results.append(result)

        # Apply merge results
        for pr_num, copied_pages, max_tail_rid in merge_results:
            pr = self.page_range_directory.get(pr_num)
            if not pr:
                continue
                
            with self._merge_lock:
                pr.base_pages = copied_pages
                pr.tps = max_tail_rid

            # Update buffer pool if exists
            if Page._bufferpool is not None:
                for col_pages in copied_pages:
                    for p in col_pages:
                        Page._bufferpool.access(p)
                        Page._bufferpool.mark_dirty(p.page_id)
                        
            # OPTIMIZATION: Invalidate cache for merged records
            keys_to_remove = [k for k in self._value_cache if k[0] in pr.base_records]
            for k in keys_to_remove:
                self._value_cache.pop(k, None)

    def _merge_page_range(self, pr_num):
        """Merge a single page range"""
        page_range = self.page_range_directory.get(pr_num)
        if page_range is None or not page_range.tail_records:
            return None

        # Find max tail RID
        max_tail_rid = max(page_range.tail_records.keys())
        if max_tail_rid <= page_range.tps:
            return None

        # OPTIMIZATION: Create pages more efficiently
        copied_pages = []
        for col in self._column_range:
            col_pages = []
            orig_col_pages = page_range.base_pages[col]
            for orig_page in orig_col_pages:
                new_page = Page()
                new_page.num_records = orig_page.num_records
                new_page.current_offset = orig_page.current_offset
                # OPTIMIZATION: Direct data copy
                new_page.data = bytearray(orig_page.data)  # Creates a copy
                col_pages.append(new_page)
            copied_pages.append(col_pages)

        # Get all base RIDs in this range
        base_rids = [
            rid for rid, entry in self.page_directory.items()
            if entry.is_base and entry.page_range_number == pr_num
        ]

        # OPTIMIZATION: Batch update columns
        updates_by_page = {}  # (col, page_num, offset) -> value
        
        for base_rid in base_rids:
            entry = self.page_directory.get(base_rid)
            if not entry:
                continue
                
            data_locs = entry.data_locations
            latest_columns = self.construct_full_record(base_rid)
            
            if not latest_columns:
                continue

            for col in range(self.num_columns):
                loc = data_locs[col + self._data_column_offset]
                if loc is not None and latest_columns[col] is not None:
                    updates_by_page[(col + self._data_column_offset, loc.page_number, loc.offset)] = latest_columns[col]

        # Apply updates in batch
        for (col, page_num, offset), value in updates_by_page.items():
            cp = copied_pages[col][page_num]
            cp.data[offset:offset + COLUMN_ENTRY_SIZE] = value.to_bytes(8, 'little')

        return (pr_num, copied_pages, max_tail_rid)

    def get_cache_stats(self):
        """Get cache hit/miss statistics"""
        total = self._cache_hits + self._cache_misses
        hit_rate = self._cache_hits / total if total > 0 else 0
        return {
            'hits': self._cache_hits,
            'misses': self._cache_misses,
            'hit_rate': hit_rate,
            'cache_size': len(self._value_cache)
        }


class PageRange:
    __slots__ = ('table', 'page_range_number', 'base_pages', 'tail_pages', 
                 'base_records', 'tail_records', 'num_records', 'tps')
                 
    def __init__(self, table, page_range_number):
        self.table = table
        self.page_range_number = page_range_number
        total_cols = table.num_columns + 3
        
        # OPTIMIZATION: Pre-allocate page lists
        self.base_pages = [[Page()] for _ in range(total_cols)]
        self.tail_pages = [[Page()] for _ in range(total_cols)]
        
        self.base_records: Dict[int, Record] = {}
        self.tail_records: Dict[int, Record] = {}
        self.num_records = 0
        self.tps = 0  # Tail pointer for merge

    def get_last_record(self, is_base):
        """Get the last record in the range"""
        records = self.base_records if is_base else self.tail_records
        if not records:
            return None
        # OPTIMIZATION: Use next(reversed()) which is O(1) for dict
        return records[next(reversed(records))]

    def get_record(self, is_base, rid):
        """Get a record by RID"""
        return self.base_records.get(rid) if is_base else self.tail_records.get(rid)

    def add_record(self, is_base, record):
        """Add a record to the range"""
        if is_base:
            self.base_records[record.rid] = record
            self.num_records += 1
        else:
            self.tail_records[record.rid] = record

    def add_page(self, is_base, column_number):
        """Add a new page to the specified column"""
        pages = self.base_pages if is_base else self.tail_pages
        pages[column_number].append(Page())
        
    def __len__(self):
        """Return number of base records"""
        return self.num_records