PAGE_SIZE = 4096
COLUMN_ENTRY_SIZE = 8
MAX_RECORDS_PER_PAGE = PAGE_SIZE // COLUMN_ENTRY_SIZE

class Page:
    _bufferpool = None   # Class-level bufferpool reference (set by Database.open)
    _next_id = 1         # Auto-incrementing page ID counter

    def __init__(self):
        self.page_id = Page._next_id
        Page._next_id += 1
        self.num_records = 0
        self.current_offset = 0
        self.data = bytearray(PAGE_SIZE)
        # Register with bufferpool if available
        if Page._bufferpool is not None:
            Page._bufferpool.access(self)

    def _ensure_loaded(self):
        """Ensure page data is in memory. Loads from disk if evicted."""
        if self.data is None:
            if Page._bufferpool is not None:
                Page._bufferpool.load_page(self)
            if self.data is None:
                # Page was never written to disk — create fresh bytearray
                self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < MAX_RECORDS_PER_PAGE

    def write(self, value, offset=None):
        self._ensure_loaded()
        # Pin the page while it's being accessed
        if Page._bufferpool is not None:
            Page._bufferpool.pin(self.page_id)
        try:
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
            # Notify bufferpool: page accessed and now dirty
            if Page._bufferpool is not None:
                Page._bufferpool.access(self)
                Page._bufferpool.mark_dirty(self.page_id)
            return True
        finally:
            # Unpin the page — transaction no longer needs it
            if Page._bufferpool is not None:
                Page._bufferpool.unpin(self.page_id)

    def read(self, index):
        self._ensure_loaded()
        # Pin the page while it's being accessed
        if Page._bufferpool is not None:
            Page._bufferpool.pin(self.page_id)
        try:
            if index >= self.num_records:
                return None
            start_offset = index * COLUMN_ENTRY_SIZE
            end_offset = start_offset + COLUMN_ENTRY_SIZE
            value_in_bytes = self.data[start_offset:end_offset]
            # Notify bufferpool: page accessed
            if Page._bufferpool is not None:
                Page._bufferpool.access(self)
            return int.from_bytes(value_in_bytes, byteorder='little')
        finally:
            # Unpin the page — transaction no longer needs it
            if Page._bufferpool is not None:
                Page._bufferpool.unpin(self.page_id)
