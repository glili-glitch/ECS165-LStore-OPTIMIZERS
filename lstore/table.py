from lstore import page
from lstore.index import Index
from time import time
from dataclasses import dataclass
from typing import List

from lstore.page import Page

RID_COLUMN = 0
INDIRECTION_COLUMN = 1
# TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 2
DATA_COLUMN_OFFSET = 3
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
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self.page_range_directory = {}
        self.RID_COLUMN = 0
        self.INDIRECTION_COLUMN = 1
        self.SCHEMA_ENCODING_COLUMN = 2
        self.DATA_COLUMN_OFFSET = 3

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
        all_columns = list(all_columns)
        if all_columns[RID_COLUMN] is None:
            all_columns[RID_COLUMN] = record.rid
        if all_columns[SCHEMA_ENCODING_COLUMN] is None:
            all_columns[SCHEMA_ENCODING_COLUMN] = 0
        if all_columns[INDIRECTION_COLUMN] is None:
            if is_base:
                all_columns[INDIRECTION_COLUMN] = record.rid
            else:
                raise Exception("Tail record missing!")

        for i, v in enumerate(all_columns):
            if v is None:
                continue
            if isinstance(v, str):
                if i == SCHEMA_ENCODING_COLUMN and set(v) <= {"0", "1"}:
                    all_columns[i] = int(v, 2)
                else:
                    all_columns[i] = int(v)
            elif not isinstance(v, int):
                all_columns[i] = int(v)

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

            # if all_columns[column_number] is None:
            #     new_record_data_locations[column_number] = None
            #     continue

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
            
            # if is None then 0
            val = all_columns[column_number]
            if val is None:
                val = 0            
            page_to_write.write(val)

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
        
        if indirection_rid == base_rid or indirection_rid == None:
            base_record = base_page_range.get_record(is_base=True, rid=base_rid)
            base_columns = base_record.columns
            new_columns = [x if x is not None else y for x, y in zip(columns, base_columns)]
            columns = new_columns
        
        return columns 

    def get_column_value(self, rid, column_number, relative_version=0):
        full_record_columns = self.construct_full_record(rid, relative_version)
        return full_record_columns[column_number]

    def __merge(self):
        print("merge is happening")
        pass

class PageRange:

    def __init__(self, table, page_range_number):
        self.table = table
        self.page_range_number = page_range_number
        self.base_pages = [[Page()] for _ in range(table.num_columns + 3)]
        self.tail_pages = [[Page()] for _ in range(table.num_columns + 3)]
        self.base_records = {}
        self.tail_records = {}
        self.num_records = 0

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

 
