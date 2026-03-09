from itertools import count
from lstore import table
from lstore import page
from lstore.table import Record

# Use a global counter for RIDs, starting at 1
_rid_counter = count(1)

class Query:
    def __init__(self, table):
        self.table = table

    def insert(self, *columns):
        # 1. Check if record already exists using the index
        pk = columns[self.table.key]
        existing_rids = self.table.index.locate(self.table.key, pk)
        if existing_rids:
            return False

        # 2. Basic validation
        if len(columns) != self.table.num_columns:
            return False

        # 3. Determine which page range this belongs to
        pr_num = pk // table.NUM_RECORDS_PER_RANGE
        if pr_num not in self.table.page_range_directory:
            self.table.add_page_range(pr_num)

        # 4. Create the record
        new_rid = next(_rid_counter)
        schema_encoding = 0
        
        # Meta columns: Indirection (self), RID (self), Schema
        # Base records point to themselves initially
        meta_and_data = [new_rid, new_rid, schema_encoding] + list(columns)
        
        new_record = Record(new_rid, pk, list(columns))
        self.table.add_record(pr_num, True, *meta_and_data, record=new_record)

        # 5. Update the index for every column
        for i, val in enumerate(columns):
            self.table.index.add_to_index(i, val, new_rid)

        return True

    def select(self, search_key, search_key_index, projected_columns_index):
        # Version 0 is always the most recent version
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        # 1. Find the base RIDs
        rid_list = self.table.index.locate(search_key_index, search_key)
        
        # Fallback: if index fails, do a manual scan (often needed in early milestones)
        if not rid_list:
            rid_list = []
            for rid, entry in self.table.page_directory.items():
                if entry.is_base:
                    val = self.table.get_column_value(rid, search_key_index, 0)
                    if val == search_key:
                        rid_list.append(rid)

        results = []
        for rid in rid_list:
            if rid not in self.table.page_directory:
                continue

            # 2. Construct the record based on the version requested
            # relative_version 0 = latest, -1 = one before, etc.
            data_columns = self.table.construct_full_record(rid, relative_version)
            pk = self.table.get_primary_key(rid)

            # 3. Apply the projection mask (e.g., [1, 0, 1] means return col 0 and 2)
            filtered_cols = []
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    filtered_cols.append(data_columns[i])

            results.append(Record(rid, pk, filtered_cols))

        return results

    def update(self, primary_key, *columns):
        # 1. Find the base record
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids:
            return False
        base_rid = rids[0]

        # 2. Don't allow updating the Primary Key
        if columns[self.table.key] is not None:
            return False

        # 3. Get metadata and location info
        entry = self.table.page_directory[base_rid]
        pr_num = entry.page_range_number
        pr = self.table.page_range_directory[pr_num]
        
        # Find where Indirection and Schema are stored
        indir_loc = entry.data_locations[table.INDIRECTION_COLUMN]
        schema_loc = entry.data_locations[table.SCHEMA_ENCODING_COLUMN]
        
        # Read old values to calculate new ones
        indir_page = pr.base_pages[table.INDIRECTION_COLUMN][indir_loc.page_number]
        old_tail_rid = indir_page.read(indir_loc.offset // page.COLUMN_ENTRY_SIZE)
        
        schema_page = pr.base_pages[table.SCHEMA_ENCODING_COLUMN][schema_loc.page_number]
        old_schema = schema_page.read(schema_loc.offset // page.COLUMN_ENTRY_SIZE)

        # 4. Create the Tail Record
        new_tail_rid = next(_rid_counter)
        
        # Update schema bitmask (1 for modified, 0 for same)
        new_schema = old_schema
        for i, val in enumerate(columns):
            if val is not None:
                new_schema |= (1 << (self.table.num_columns - 1 - i))

        # New Tail Record points to the PREVIOUS Tail Record (or base if first update)
        # format: [Indirection, RID, Schema, ...data...]
        meta_and_data = [old_tail_rid, new_tail_rid, new_schema] + list(columns)
        
        tail_rec_obj = Record(new_tail_rid, primary_key, list(columns))
        self.table.add_record(pr_num, False, *meta_and_data, record=tail_rec_obj)

        # 5. Update Base Record's Indirection and Schema
        indir_page.write(new_tail_rid, indir_loc.offset)
        schema_page.write(new_schema, schema_loc.offset)

        # 6. Housekeeping
        self.table.trigger_merge_check()
        return True

    def sum(self, start_range, end_range, aggregate_column_index):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        total = 0
        any_found = False

        # Loop through the range of keys
        for key in range(start_range, end_range + 1):
            rids = self.table.index.locate(self.table.key, key)
            if not rids:
                continue

            rid = rids[0]
            if rid not in self.table.page_directory:
                continue

            any_found = True
            # Get the value for the specific version
            val = self.table.get_column_value(rid, aggregate_column_index, relative_version)
            if val is not None:
                total += val

        return total if any_found else False

    def delete(self, primary_key):
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids: return False
        
        base_rid = rids[0]
        # To delete, we set everything to None (or a special value) and remove from directory
        if base_rid in self.table.page_directory:
            del self.table.page_directory[base_rid]
            return True
        return False