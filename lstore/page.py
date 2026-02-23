PAGE_SIZE = 4096
COLUMN_ENTRY_SIZE = 8
HEADER_SIZE = 8
MAX_RECORDS_PER_PAGE = (PAGE_SIZE - HEADER_SIZE) // COLUMN_ENTRY_SIZE

class Page:

    def __init__(self):
        self.num_records = 0
        self.current_offset = 0
        self.data = bytearray(PAGE_SIZE)
        self._write_header()

    def has_capacity(self):
        return self.num_records < MAX_RECORDS_PER_PAGE
    
    def write(self, value, offset=None):
        if value is None:
            return True
        if offset is None:
            if not self.has_capacity():
                return False
            offset = self.current_offset
            self.current_offset += COLUMN_ENTRY_SIZE
            self.num_records += 1
            self._write_header()
        # check offset (data area) 
        if offset < 0 or offset % COLUMN_ENTRY_SIZE !=0:
            return False

        real_offset = HEADER_SIZE + offset   
        if real_offset + COLUMN_ENTRY_SIZE > PAGE_SIZE:
            return False
            
        # if offset == self.num_records * COLUMN_ENTRY_SIZE:
        #     self.num_records += 1
        #     self.current_offset += COLUMN_ENTRY_SIZE
        value_in_bytes = value.to_bytes(8, byteorder='little', signed = True)
        self.data[real_offset : real_offset + COLUMN_ENTRY_SIZE] = value_in_bytes
        return True
    
    def read(self, index):
        if index < 0 or index >= self.num_records:
            return None
        start_offset = HEADER_SIZE + index * COLUMN_ENTRY_SIZE
        end_offset = start_offset + COLUMN_ENTRY_SIZE
        value_in_bytes = self.data[start_offset:end_offset]
        return int.from_bytes(value_in_bytes, byteorder='little' , signed = True)
    # Serialization APIs
    # tanslate page to writing to the file
    def to_bytes(self):
        # change the page to exactly PAGE_SIZE bytes.
        if len(self.data) != PAGE_SIZE:
            raise ValueError(f"Page data lenght is not expected page size!")
        self._write_header()
        return bytes(self.data)

    @classmethod
    # translate file form the disk to it page
    def from_bytes(cls, page_bytes: bytes):
        if len(page_bytes) != PAGE_SIZE:
            raise ValueError(f"Note expected page bytes lenght!")
        page = cls()
        page.data = bytearray(page_bytes)
        page._read_header()
        if page.num_records > MAX_RECORDS_PER_PAGE:
            page.num_records = MAX_RECORDS_PER_PAGE
            page.current_offset = page.num_records * COLUMN_ENTRY_SIZE
            page._write_header()
        return page

        # #cauclate num_records by searching for nono-zero 8-bytes slots.
        # num = 0
        # zero = b"\x00" * COLUMN_ENTRY_SIZE
        # for off in range(0, PAGE_SIZE,COLUMN_ENTRY_SIZE):
        #     if page.data[off:off + COLUMN_ENTRY_SIZE] != zero:
        #         num += 1

        # page.num_records = num
        # page.current_offset = num * COLUMN_ENTRY_SIZE
        # return page

    # add writer and read function when page is created and the number_records changes 
    def _write_header(self):
        self.data[0:8] = int(self.num_records).to_bytes(8, byteorder = 'little', signed = False)
    def _read_header(self):
        self.num_records = int.from_bytes(self.data[0:8], byteorder = 'little', signed = False)
        self.current_offset = self.num_records * COLUMN_ENTRY_SIZE

