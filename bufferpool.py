from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock

from lstore.page import Page


@dataclass
class BufferFrame:
    table_name: str
    page_id: str
    page: Page
    pin_count: int = 0
    dirty: bool = False


class BufferPool:
    def __init__(self, disk_manager, capacity_pages=64):
        if capacity_pages <= 0:
            raise ValueError("BufferPool capacity_pages must be > 0")
        self.disk_manager = disk_manager
        self.capacity_pages = capacity_pages
        self._frames = OrderedDict()  # key -> BufferFrame, ordered by LRU
        self._lock = RLock()
        self.eviction_count = 0
        self.hit_count = 0
        self.miss_count = 0

    def _key(self, table_name, page_id):
        return f"{table_name}:{page_id}"

    def fetch_page(self, table_name, page_id, create_if_missing=True):
        key = self._key(table_name, page_id)
        with self._lock:
            frame = self._frames.get(key)
            if frame is not None:
                self.hit_count += 1
                frame.pin_count += 1
                self._frames.move_to_end(key)
                return frame.page

            self.miss_count += 1
            if len(self._frames) >= self.capacity_pages:
                self._evict_one_locked()

            page = self.disk_manager.read_page(table_name, page_id)
            if page is None:
                if not create_if_missing:
                    return None
                page = Page()

            frame = BufferFrame(table_name=table_name, page_id=page_id, page=page, pin_count=1)
            self._frames[key] = frame
            self._frames.move_to_end(key)
            return frame.page

    def unpin_page(self, table_name, page_id, dirty=False):
        key = self._key(table_name, page_id)
        with self._lock:
            frame = self._frames.get(key)
            if frame is None:
                return False
            if frame.pin_count > 0:
                frame.pin_count -= 1
            if dirty:
                frame.dirty = True
            return True

    def flush_page(self, table_name, page_id):
        key = self._key(table_name, page_id)
        with self._lock:
            frame = self._frames.get(key)
            if frame is None:
                return False
            if frame.dirty:
                self.disk_manager.write_page(table_name, page_id, frame.page)
                frame.dirty = False
            return True

    def flush_all(self):
        with self._lock:
            for frame in self._frames.values():
                if frame.dirty:
                    self.disk_manager.write_page(frame.table_name, frame.page_id, frame.page)
                    frame.dirty = False

    def get_frame(self, table_name, page_id):
        key = self._key(table_name, page_id)
        with self._lock:
            return self._frames.get(key)

    def _evict_one_locked(self):
        
        for key, frame in list(self._frames.items()):
            if frame.pin_count == 0:
                if frame.dirty:
                    self.disk_manager.write_page(frame.table_name, frame.page_id, frame.page)
                    frame.dirty = False
                self._frames.pop(key, None)
                self.eviction_count += 1
                return
        raise RuntimeError("BufferPool is full and all pages are pinned")