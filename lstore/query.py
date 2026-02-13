from lstore import table
from lstore import page
from lstore.table import Table, Record
from lstore.index import Index
from itertools import count
_rid_counter = count(1)



class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # use index to get RID of base record
        # call update with all columns set to None to insert tail record of all nulls
        # remove primary key from index, and any mapping from the old column values to RID in other indices
        # remove RID of base record from page directory
        rids = self.table.index.locate(self.table.key, primary_key)
        if not rids: 
            return False
        base_rid = rids[0]
        record = self.table.construct_full_record(base_rid)
        original_columns = record.columns

        self.update(primary_key, *[None] * self.table.num_columns)

        for i, column in enumerate(original_columns):
            self.table.index.remove_from_index(i, column, base_rid)

        del self.table.page_directory[base_rid]
        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):

        if len(self.table.index.locate(self.table.key, columns[self.table.key])) > 0:
            return False
    
        # check if col if is correct number
        if len(columns) != self.table.num_columns:
            return False

        # use the key to find a pageRange
        primary_key = columns[self.table.key]
        # calculate range number using primary key
        page_range_number = primary_key // table.NUM_RECORDS_PER_RANGE

        # if there isn't page number then we build a new page
        if page_range_number not in self.table.page_range_directory:
            self.table.add_page_range(page_range_number)

        page_range = self.table.page_range_directory[page_range_number]

        # allocate  RID
        rid = next(_rid_counter)
        # print(rid)

        # schema for base record
        schema_encoding = '0' * self.table.num_columns
                    
        # create new record object with new RID
        # call add record in table class
        record = Record(rid, primary_key, list(columns))
        # construct variable that holds all columns including metadata
        all_columns = [rid, None, int(schema_encoding, 2)] + list(columns)

        self.table.add_record(page_range_number, True, *all_columns, record=record)
        
        for i, column in enumerate(columns):
            self.table.index.add_to_index(i, column, rid)

        return True

    """
    # Read matching record with specified search key

    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        rid_list = self.table.index.locate(search_key_index, search_key)
        record_list = []
        for rid in rid_list:
            columns = self.table.construct_full_record(rid)
            primary_key = self.table.get_primary_key(rid)
            new_columns = []
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 0:
                    continue
                new_columns.append(columns[i])
            record_list.append(Record(rid, primary_key, new_columns))
        return record_list
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rid_list = self.table.index.locate(search_key_index, search_key)
        record_list = []
        for rid in rid_list:
            columns = self.table.construct_full_record(rid, relative_version * -1)
            primary_key = self.table.get_primary_key(rid)
            new_columns = []
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 0:
                    continue
                new_columns.append(columns[i])
            record_list.append(Record(rid, primary_key, new_columns))
        return record_list
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # IMPORTANT: must check if columns are all set to null, if so then you are doing a delete operation and SE should be all 0's
        # use index to get RID of base record
        # use page directory to get data locations of base record
        # create new tail record object
        # construct variable that holds all columns including metadata 
        # update indirection pointer and schema encoding of base record 
        # call add record in table class 
        # (note: if record is being updated for the first time, must add copy of base record as tail record)

        rids = self.table.index.locate(self.table.key,primary_key)
        if not rids:
            return False
        base_rid = rids[0]

        if columns[self.table.key] is not None and len(self.table.index.locate(self.table.key, columns[self.table.key])) > 0:
            return False

        base_page_directory_entry = self.table.page_directory[base_rid]
        base_page_range_number = base_page_directory_entry.page_range_number
        base_page_range = self.table.page_range_directory[base_page_range_number]
        base_data_locations = base_page_directory_entry.data_locations
        if base_data_locations[table.INDIRECTION_COLUMN] is None:
            base_indirection_page_number = base_page_range.base_pages[table.INDIRECTION_COLUMN].__len__() - 1
            base_indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][base_indirection_page_number]
            base_indirection_offset = base_indirection_page.current_offset
        else:
            base_indirection_page_number = base_data_locations[table.INDIRECTION_COLUMN].page_number
            base_indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][base_indirection_page_number]
            base_indirection_offset = base_data_locations[table.INDIRECTION_COLUMN].offset

        base_schema_page_number = base_data_locations[table.SCHEMA_ENCODING_COLUMN].page_number
        base_schema_page = base_page_range.base_pages[table.SCHEMA_ENCODING_COLUMN][base_schema_page_number]
        base_schema_offset = base_data_locations[table.SCHEMA_ENCODING_COLUMN].offset
        base_schema = format(base_schema_page.read(base_schema_offset // page.COLUMN_ENTRY_SIZE), f"0{self.table.num_columns}b")

        base_indirection = base_indirection_page.read(base_indirection_offset // page.COLUMN_ENTRY_SIZE)

        #schema encoding

        schema = ""
        new_base_schema = base_schema
        for i, v in enumerate(columns):
            if v is None:
                schema += "0"
            else:
                schema += "1"
                new_base_schema = new_base_schema[:i] + "1" + new_base_schema[i+1:]
        
        if columns == [None] * self.table.num_columns:
            new_base_schema = '0' * self.table.num_columns
            schema = '0' * self.table.num_columns

        copy_tail_record = None

        if base_schema == '0' * self.table.num_columns and columns != [None] * self.table.num_columns:
            copy_columns = [None] * self.table.num_columns
            for i, column in enumerate(columns):
                if column is not None:
                    base_page_number = base_data_locations[i + 3].page_number
                    base_page = base_page_range.base_pages[i + 3][base_page_number]
                    base_offset = base_data_locations[i + 3].offset
                    column_value = base_page.read(base_offset // page.COLUMN_ENTRY_SIZE)
                    copy_columns[i] = column_value
                else:
                    copy_columns[i] = None 
            copy_tail_record = Record(next(_rid_counter), primary_key, copy_columns)
            copy_all_columns = [copy_tail_record.rid, base_rid, int(schema, 2)] + copy_columns
            self.table.add_record(base_page_range_number, False, *copy_all_columns, record=copy_tail_record)
        
        tail_record = Record(next(_rid_counter), primary_key, list(columns))
        tail_indirection = None
        if base_schema == '0' * self.table.num_columns and columns != [None] * self.table.num_columns:
            tail_indirection = copy_tail_record.rid
        else:
            tail_indirection = base_indirection

        if columns == [None] * self.table.num_columns:
            tail_indirection = base_rid

        all_columns = [tail_record.rid, tail_indirection, int(schema, 2)] + list(columns)
        self.table.add_record(base_page_range_number, False, *all_columns, record=tail_record)

        if base_indirection_offset == page.PAGE_SIZE:
            base_indirection_page_number += 1
            base_page_range.add_page(True, table.INDIRECTION_COLUMN)
            base_indirection_page = base_page_range.base_pages[table.INDIRECTION_COLUMN][base_indirection_page_number]
            base_indirection_offset = 0

        base_indirection_page.write(tail_record.rid, base_indirection_offset)
        base_schema_page.write(int(new_base_schema, 2), base_schema_offset)

        base_page_directory_entry.data_locations[table.INDIRECTION_COLUMN] = table.PageCoord(base_indirection_page_number, base_indirection_offset)

        current_columns = self.table.construct_full_record(base_rid)
        previous_columns = self.table.construct_full_record(base_rid, 1)

        for i, (curr_val, prev_val) in enumerate(zip(current_columns, previous_columns)):
            if curr_val != prev_val:
                self.table.index.remove_from_index(i, prev_val, base_rid)  # Remove OLD value
                self.table.index.add_to_index(i, curr_val, base_rid)       # Add NEW value

        return True 
            
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0
        has_records = False
        for key in range(start_range, end_range + 1):
            rids = self.table.index.locate(0, key)
            if rids is None:
                continue
            rid = rids[0]
            has_records = True
            column_value = self.table.get_column_value(rid, aggregate_column_index)
            if column_value is None:
                column_value = 0
            sum += column_value
        if not has_records:
            return False
        return sum
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        sum = 0
        has_records = False
        column_values = []
        for key in range(start_range, end_range + 1):
            rids = self.table.index.locate(0, key)
            if rids is None or len(rids) == 0:
                continue
            rid = rids[0]
            has_records = True
            column_value = self.table.get_column_value(rid, aggregate_column_index, relative_version * -1)
            column_values.append(column_value)
            if column_value is None:
                column_value = 0
            sum += column_value
        if not has_records:
            return False
        return sum

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
