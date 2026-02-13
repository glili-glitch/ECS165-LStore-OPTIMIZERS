PAGE_SIZE = 4096
COLUMN_ENTRY_SIZE = 8
MAX_RECORDS_PER_PAGE = PAGE_SIZE // COLUMN_ENTRY_SIZE

class Page:

    def __init__(self):
        self.num_records = 0
        self.current_offset = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < MAX_RECORDS_PER_PAGE
    
    def write(self, value, offset=None):
        if value is None:
            return True
        if offset is None:
            offset = self.current_offset
            self.current_offset += COLUMN_ENTRY_SIZE
            self.num_records += 1
        if offset + COLUMN_ENTRY_SIZE > PAGE_SIZE:
            return False
        if offset == self.num_records * COLUMN_ENTRY_SIZE:
            self.num_records += 1
            self.current_offset += COLUMN_ENTRY_SIZE
        value_in_bytes = value.to_bytes(8, byteorder='little')
        self.data[offset : offset + COLUMN_ENTRY_SIZE] = value_in_bytes
        return True
    
    def read(self, index):
        if index >= self.num_records:
            return None
        start_offset = index * COLUMN_ENTRY_SIZE
        end_offset = start_offset + COLUMN_ENTRY_SIZE
        value_in_bytes = self.data[start_offset:end_offset]
        return int.from_bytes(value_in_bytes, byteorder='little')

