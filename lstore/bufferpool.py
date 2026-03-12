"""
Buffer Pool Manager for L-Store
Implements LRU page replacement with dirty page tracking and pin/unpin support.
"""
from collections import OrderedDict
import os


class BufferPool:
    DEFAULT_POOL_SIZE = 1000

import threading


class BufferPool:
    DEFAULT_POOL_SIZE = 1000

    def __init__(self, pool_size=None, disk_path=None):
        self.pool_size = pool_size or self.DEFAULT_POOL_SIZE
        self.disk_path = disk_path
        # OrderedDict maintains insertion/access order for LRU tracking
        self.pool = OrderedDict()       # page_id -> Page object
        self.dirty_pages = set()        # set of dirty page_ids
        self.pin_counts = {}            # page_id -> pin count
        self.pool_lock = threading.RLock() # To serialize management of the pool
        self.page_latches = {}          # page_id -> threading.Lock for I/O serialization
        if disk_path:
            os.makedirs(os.path.join(disk_path, 'pages'), exist_ok=True)

    def _get_page_latch(self, page_id):
        with self.pool_lock:
            if page_id not in self.page_latches:
                self.page_latches[page_id] = threading.Lock()
            return self.page_latches[page_id]

    

    def access(self, page):
        """Register a page access. Moves to MRU position or adds to pool."""
        pid = page.page_id
        with self.pool_lock:
            if pid in self.pool:
                self.pool.move_to_end(pid)          # Mark as most recently used
            else:
                self._ensure_capacity_unlocked()
                self.pool[pid] = page
                self.pool.move_to_end(pid)

    def load_page(self, page):
        """Load an evicted page's data from its on-disk file."""
        latch = self._get_page_latch(page.page_id)
        with latch:
            if page.data is not None: return True # Already loaded by another thread
            page_path = self._get_page_path(page.page_id)
            if page_path and os.path.exists(page_path):
                with open(page_path, 'rb') as f:
                    data = f.read()
                page.data = bytearray(data)
                
                with self.pool_lock:
                    self._ensure_capacity_unlocked()
                    self.pool[page.page_id] = page
                    self.pool.move_to_end(page.page_id)
                return True
            return False

    

    def mark_dirty(self, page_id):
        """Mark a page as dirty (modified in memory, differs from disk)."""
        with self.pool_lock:
            self.dirty_pages.add(page_id)

    

    def pin(self, page_id):
        """Pin a page — prevents eviction while pin count > 0."""
        with self.pool_lock:
            self.pin_counts[page_id] = self.pin_counts.get(page_id, 0) + 1

    def unpin(self, page_id):
        """Unpin a page — allows eviction once pin count reaches 0."""
        with self.pool_lock:
            if page_id in self.pin_counts:
                self.pin_counts[page_id] -= 1
                if self.pin_counts[page_id] <= 0:
                    del self.pin_counts[page_id]

    def is_pinned(self, page_id):
        with self.pool_lock:
            return self.pin_counts.get(page_id, 0) > 0

    

    def _ensure_capacity_unlocked(self):
        """Evict LRU unpinned pages until pool is under capacity. (Assumes pool_lock held)"""
        while len(self.pool) >= self.pool_size:
            evicted = False
            for pid in list(self.pool.keys()):       # Iterate LRU → MRU
                if not self.is_pinned_unlocked(pid):
                    self._evict_unlocked(pid)
                    evicted = True
                    break
            if not evicted:
                break   # All pages pinned — cannot evict

    def is_pinned_unlocked(self, page_id):
        return self.pin_counts.get(page_id, 0) > 0

    def _evict_unlocked(self, page_id):
        """Evict a single page. Flush to disk first if dirty. (Assumes pool_lock held)"""
        page = self.pool.get(page_id)
        if page is None:
            return
        
       
        
        if page_id in self.dirty_pages:
            self._write_page_to_disk_unlocked(page)
            self.dirty_pages.discard(page_id)
        page.data = None                            # Free memory
        del self.pool[page_id]

    

    def flush_all(self):
        """Write all dirty pages to disk (called on db.close)."""
        with self.pool_lock:
            for pid in list(self.dirty_pages):
                page = self.pool.get(pid)
                if page is not None and page.data is not None:
                    self._write_page_to_disk_unlocked(page)
            self.dirty_pages.clear()

    def write_all_pages(self):
        """Write every in-memory page to disk (for full persistence)."""
        with self.pool_lock:
            for pid, page in self.pool.items():
                if page.data is not None:
                    self._write_page_to_disk_unlocked(page)
            self.dirty_pages.clear()

    def _write_page_to_disk_unlocked(self, page):
        """Writes page to disk. Assumes pool_lock or page latch is managed appropriately."""
       
        latch = self._get_page_latch(page.page_id)
       
        with latch:
            page_path = self._get_page_path(page.page_id)
            if page_path:
                with open(page_path, 'wb') as f:
                    f.write(bytes(page.data))

    def _get_page_path(self, page_id):
        if self.disk_path is None:
            return None
        return os.path.join(self.disk_path, 'pages', f'page_{page_id}.bin')

   

    def get_stats(self):
        return {
            'pool_size': self.pool_size,
            'pages_in_pool': len(self.pool),
            'dirty_pages': len(self.dirty_pages),
            'pinned_pages': sum(1 for c in self.pin_counts.values() if c > 0),
        }
