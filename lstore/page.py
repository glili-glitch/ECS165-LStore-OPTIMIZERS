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
    # Serialization APIs
    # tanslate page to writing to the file
    def to_bytes(self) -> bytes:
        # change the page to exactly PAGE_SIZE bytes.
        if len(self.data) ! = PAGE_SIZE:
            raise ValueError(f"Page data lenght is not expected page size!")
        return bytes(self.data)

    @classmethod
    # translate file form the disk to it page
    def from_bytes(cls, page_bytes: bytes) -> "Page":
        if len(page_bytes) != PAGE_SIZE:
            raise ValueError(f"Note expected page bytes lenght!")
        page = cls()
        page.data = bytearray(page_bytes)

        #cauclate num_records by searching for nono-zero 8-bytes slots.
        num = 0
        zero = b"\x00" * COLUMN_ENTRY_SIZE
        for off in range(0, PAGE_SIZE,COLUMN_ENTRY_SIZE):
            if page.data[off:off + COLUMN_ENTRY_SIZE] ! = zero:
                num += 1

        page.num_records = num
        page.current_offset = num * COLUMN_ENTRY_SIZE
        return page




    



# test