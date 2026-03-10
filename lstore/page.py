PAGE_SIZE = 4096
COLUMN_ENTRY_SIZE = 8
MAX_RECORDS_PER_PAGE = PAGE_SIZE // COLUMN_ENTRY_SIZE


class Page:
    _bufferpool = None
    _next_id = 1

    def __init__(self, page_id=None):
        if page_id is None:
            self.page_id = Page._next_id
            Page._next_id += 1
        else:
            self.page_id = page_id
            if page_id >= Page._next_id:
                Page._next_id = page_id + 1

        self.num_records = 0
        self.current_offset = 0
        self.data = bytearray(PAGE_SIZE)

        if Page._bufferpool is not None:
            Page._bufferpool.access(self)

    def _ensure_loaded(self):
        if self.data is None:
            if Page._bufferpool is not None:
                Page._bufferpool.load_page(self)
            if self.data is None:
                self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < MAX_RECORDS_PER_PAGE

    def write(self, value, offset=None):
        self._ensure_loaded()

        if Page._bufferpool is not None:
            Page._bufferpool.pin(self.page_id)

        try:
            if value is None:
                return True

            if offset is None:
                offset = self.current_offset
                if offset + COLUMN_ENTRY_SIZE > PAGE_SIZE:
                    return False
                self.current_offset += COLUMN_ENTRY_SIZE
                self.num_records += 1
            else:
                if offset + COLUMN_ENTRY_SIZE > PAGE_SIZE:
                    return False

                # if writing exactly at the logical end, extend the page
                if offset == self.current_offset:
                    self.current_offset += COLUMN_ENTRY_SIZE
                    self.num_records += 1

            value_in_bytes = int(value).to_bytes(8, byteorder='little', signed=True)
            self.data[offset: offset + COLUMN_ENTRY_SIZE] = value_in_bytes

            if Page._bufferpool is not None:
                Page._bufferpool.access(self)
                Page._bufferpool.mark_dirty(self.page_id)

            return True
        finally:
            if Page._bufferpool is not None:
                Page._bufferpool.unpin(self.page_id)

    def read(self, index):
        self._ensure_loaded()

        if Page._bufferpool is not None:
            Page._bufferpool.pin(self.page_id)

        try:
            if index >= self.num_records:
                return None

            start_offset = index * COLUMN_ENTRY_SIZE
            end_offset = start_offset + COLUMN_ENTRY_SIZE
            value_in_bytes = self.data[start_offset:end_offset]

            if Page._bufferpool is not None:
                Page._bufferpool.access(self)

            return int.from_bytes(value_in_bytes, byteorder='little', signed=True)
        finally:
            if Page._bufferpool is not None:
                Page._bufferpool.unpin(self.page_id)