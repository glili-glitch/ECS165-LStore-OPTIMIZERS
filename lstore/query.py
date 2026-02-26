from lstore import table
from lstore import page
from lstore.table import Table, Record
from lstore.index import Index
from itertools import count
import functools

_rid_counter = count(1)


class Query:
    """
    Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    __slots__ = ('table', '_num_columns', '_key_index', '_page_dir_cache')

    def __init__(self, table):
        self.table = table
        self._num_columns = table.num_columns
        self._key_index = table.key
        self._page_dir_cache = {}  # Cache for frequently accessed page directory entries

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

        # OPTIMIZATION: Direct tombstone without full update
        # Instead of calling update with all Nones, do minimal work
        
        # Get base entry
        base_entry = self.table.page_directory.get(base_rid)
        if not base_entry:
            return False
            
        # Mark as deleted by updating indirection to point to itself
        base_page_range_number = base_entry.page_range_number
        base_page_range = self.table.page_range_directory[base_page_range_number]
        base_data_locations = base_entry.data_locations
        
        # Update indirection to point to itself (tombstone marker)
        if base_data_locations[table.INDIRECTION_COLUMN]:
            indirection_page_num = base_data_locations[table.INDIRECTION_COLUMN].page_number
            indirection_offset = base_data_locations[table.INDIRECTION_COLUMN].offset
            indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][indirection_page_num]
            indirection_page.write(base_rid, indirection_offset)  # Point to self
        
        # Update schema to 0 (all columns invalid)
        schema_page_num = base_data_locations[table.SCHEMA_ENCODING_COLUMN].page_number
        schema_offset = base_data_locations[table.SCHEMA_ENCODING_COLUMN].offset
        schema_page = base_page_range.base_pages[table.SCHEMA_ENCODING_COLUMN][schema_page_num]
        schema_page.write(0, schema_offset)
        
        # Remove from indexes
        for i in range(self._num_columns):
            val = self.table.get_column_value(base_rid, i)
            if val is not None:
                self.table.index.remove_from_index(i, val, base_rid)
        
        # Remove from page directory (optional - could keep with tombstone flag)
        # del self.table.page_directory[base_rid]
        
        self.table.trigger_merge_check()
        return True

    """
    Insert a record with specified columns
    Return True upon succesful insertion
    Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # Early validation
        if len(columns) != self._num_columns:
            return False

        primary_key = columns[self._key_index]
        
        # OPTIMIZATION: Quick existence check
        existing = self.table.index.locate(self.table.key, primary_key)
        if existing:
            return False

        # Calculate page range
        page_range_number = primary_key // table.NUM_RECORDS_PER_RANGE

        # Lazy page range creation
        page_range = self.table.page_range_directory.get(page_range_number)
        if page_range is None:
            page_range = self.table.add_page_range(page_range_number)

        rid = next(_rid_counter)

        schema_encoding = 0  # OPTIMIZATION: Use int directly instead of bit string
        record = Record(rid, primary_key, list(columns))

        # Batch insert into page range
        all_columns = [rid, None, schema_encoding] + list(columns)
        self.table.add_record(page_range_number, True, *all_columns, record=record)

        # Batch index updates
        indices_to_update = [(i, columns[i], rid) for i in range(self._num_columns)]
        for i, val, rid in indices_to_update:
            self.table.index.add_to_index(i, val, rid)

        return True

    def _scan_table_for_key(self, search_key, search_key_index):
        """Helper method for full table scan when index missing"""
        rid_list = []
        page_dir = self.table.page_directory
        
        for rid, entry in page_dir.items():
            if not entry.is_base:
                continue
            
            # OPTIMIZATION: Direct column access without constructing full record
            # This assumes get_column_value is efficient
            col_val = self.table.get_column_value(rid, search_key_index)
            if col_val == search_key:
                rid_list.append(rid)
                
        return rid_list

    def _build_record_list(self, rid_list, projected_columns_index, version=0):
        """Helper to build record list with projection"""
        record_list = []
        page_dir = self.table.page_directory
        
        for rid in rid_list:
            entry = page_dir.get(rid)
            if not entry or not entry.is_base:
                continue

            # OPTIMIZATION: Only get needed columns
            if version == 0:
                columns = self.table.construct_full_record(rid)
            else:
                columns = self.table.construct_full_record(rid, version)
            
            primary_key = columns[self._key_index]

            # OPTIMIZATION: List comprehension for projection
            new_columns = [columns[i] for i in range(self._num_columns) 
                          if projected_columns_index[i] != 0]

            record_list.append(Record(rid, primary_key, new_columns))
        
        return record_list

    """
    Read matching record with specified search key
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        # Try index first
        rid_list = self.table.index.locate(search_key_index, search_key)
        
        # Fall back to scan if needed
        if rid_list is None:
            rid_list = self._scan_table_for_key(search_key, search_key_index)

        return self._build_record_list(rid_list, projected_columns_index)

    """
    Read matching record with specified search key (versioned)
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rid_list = self.table.index.locate(search_key_index, search_key)
        
        if rid_list is None:
            rid_list = self._scan_table_for_key(search_key, search_key_index)

        # OPTIMIZATION: Pre-compute version once
        version = relative_version * -1
        return self._build_record_list(rid_list, projected_columns_index, version)

    def _prepare_update_data(self, base_rid, columns):
        """Helper to prepare data for update"""
        base_entry = self.table.page_directory[base_rid]
        base_page_range_number = base_entry.page_range_number
        base_page_range = self.table.page_range_directory[base_page_range_number]
        base_data_locations = base_entry.data_locations
        
        # Get base schema
        schema_coord = base_data_locations[table.SCHEMA_ENCODING_COLUMN]
        schema_page = base_page_range.base_pages[table.SCHEMA_ENCODING_COLUMN][schema_coord.page_number]
        base_schema_int = schema_page.read(schema_coord.offset // page.COLUMN_ENTRY_SIZE)
        
        # Get base indirection
        indirection_coord = base_data_locations[table.INDIRECTION_COLUMN]
        if indirection_coord is None:
            # Create new indirection page if needed
            indirection_page_num = len(base_page_range.base_pages[table.INDIRECTION_COLUMN]) - 1
            indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][indirection_page_num]
            indirection_offset = indirection_page.current_offset
            base_indirection = None
        else:
            indirection_page_num = indirection_coord.page_number
            indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][indirection_page_num]
            indirection_offset = indirection_coord.offset
            base_indirection = indirection_page.read(indirection_offset // page.COLUMN_ENTRY_SIZE)
            
        return (base_entry, base_page_range, base_data_locations, 
                base_schema_int, base_indirection, indirection_page, 
                indirection_offset, schema_page, schema_coord)

    def _update_indexes(self, base_rid, columns, old_values_cache):
        """Batch update indexes for changed columns"""
        for i, new_val in enumerate(columns):
            if new_val is not None:
                prev_val = old_values_cache[i]
                if prev_val is not None and new_val != prev_val:
                    self.table.index.remove_from_index(i, prev_val, base_rid)
                    self.table.index.add_to_index(i, new_val, base_rid)

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
        if columns[self._key_index] is not None:
            return False

        # OPTIMIZATION: Quick check for no-op update
        cols = list(columns)
        all_none = all(v is None for v in cols)
        
        # Prepare update data
        (base_entry, base_page_range, base_data_locations, 
         base_schema_int, base_indirection, indirection_page, 
         indirection_offset, schema_page, schema_coord) = self._prepare_update_data(base_rid, cols)

        # Compute schema changes efficiently
        schema_int = 0
        new_base_schema_int = base_schema_int
        
        # OPTIMIZATION: Bit manipulation for schema
        for i, v in enumerate(cols):
            if v is not None:
                bit_mask = 1 << (self._num_columns - 1 - i)
                schema_int |= bit_mask
                new_base_schema_int |= bit_mask

        # Handle delete case
        if all_none:
            schema_int = 0
            new_base_schema_int = 0

        base_schema_is_zero = (base_schema_int == 0)
        copy_tail_record = None

        # OPTIMIZATION: Cache old values for index updates
        old_values_cache = {}
        
        # First update: create copy tail record (only if NOT delete)
        if base_schema_is_zero and not all_none:
            copy_columns = [None] * self._num_columns
            for i, column in enumerate(cols):
                if column is not None:
                    # Cache old value
                    old_values_cache[i] = self.table.get_column_value(base_rid, i, 1)
                    copy_columns[i] = old_values_cache[i]
                else:
                    copy_columns[i] = None

            copy_tail_record = Record(next(_rid_counter), primary_key, copy_columns)
            copy_all_columns = [copy_tail_record.rid, base_rid, schema_int] + copy_columns
            self.table.add_record(base_page_range.page_range_number, False, *copy_all_columns, record=copy_tail_record)
        else:
            # Cache old values for non-copy case
            for i, v in enumerate(cols):
                if v is not None:
                    old_values_cache[i] = self.table.get_column_value(base_rid, i, 1)

        # Create tail record
        tail_record = Record(next(_rid_counter), primary_key, cols)

        if all_none:
            tail_indirection = base_rid
        else:
            tail_indirection = copy_tail_record.rid if (base_schema_is_zero and copy_tail_record is not None) else base_indirection

        all_columns = [tail_record.rid, tail_indirection, schema_int] + cols
        self.table.add_record(base_page_range.page_range_number, False, *all_columns, record=tail_record)

        # Ensure indirection page has space
        if indirection_offset == page.PAGE_SIZE:
            indirection_page_num += 1
            base_page_range.add_page(True, table.INDIRECTION_COLUMN)
            indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][indirection_page_num]
            indirection_offset = 0

        # Update base indirection + schema
        indirection_page.write(tail_record.rid, indirection_offset)
        schema_page.write(new_base_schema_int, schema_coord.offset)

        base_entry.data_locations[table.INDIRECTION_COLUMN] = table.PageCoord(indirection_page_num, indirection_offset)

        # Handle index updates
        if all_none:
            # Quick delete path
            self.table.index.remove_from_index(self.table.key, primary_key, base_rid)
        else:
            # Batch update indexes
            self._update_indexes(base_rid, cols, old_values_cache)

        self.table.trigger_merge_check()
        return True

    """
    Sum aggregation over primary key range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        total = 0
        has_records = False
        
        # OPTIMIZATION: Batch key lookup
        keys = range(start_range, end_range + 1)
        for key in keys:
            rids = self.table.index.locate(self.table.key, key)
            if not rids:
                continue
            rid = rids[0]
            
            entry = self.table.page_directory.get(rid)
            if not entry or not entry.is_base:
                continue

            has_records = True
            column_value = self.table.get_column_value(rid, aggregate_column_index)
            if column_value is not None:
                total += column_value

        return total if has_records else False

    """
    Sum aggregation over primary key range (versioned)
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        total = 0
        has_records = False
        version = relative_version * -1

        keys = range(start_range, end_range + 1)
        for key in keys:
            rids = self.table.index.locate(self.table.key, key)
            if not rids:
                continue
            rid = rids[0]

            entry = self.table.page_directory.get(rid)
            if not entry or not entry.is_base:
                continue

            has_records = True
            column_value = self.table.get_column_value(rid, aggregate_column_index, version)
            if column_value is not None:
                total += column_value

        return total if has_records else False

    """
    Increments one column of the record
    """
    def increment(self, key, column):
        # OPTIMIZATION: Direct update without full select
        rids = self.table.index.locate(self.table.key, key)
        if not rids:
            return False
        
        rid = rids[0]
        current_value = self.table.get_column_value(rid, column)
        if current_value is None:
            return False
            
        updated_columns = [None] * self._num_columns
        updated_columns[column] = current_value + 1
        return self.update(key, *updated_columns)