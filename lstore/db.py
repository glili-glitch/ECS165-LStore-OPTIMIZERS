import os
import pickle
from itertools import count
from lstore.table import Table
from lstore.index import Index
from lstore.bufferpool import BufferPool
from lstore.page import Page


class Database():

    def __init__(self):
        self.tables = []
        self._table_map = {}
        self.path = None
        self.bufferpool = None

    def open(self, path):
        """
        Initialize the bufferpool and load tables from disk if data exists.
        Rebuilds indexes and resets the RID counter.
        """
        self.path = path
        os.makedirs(path, exist_ok=True)

        # Create and activate bufferpool
        self.bufferpool = BufferPool(disk_path=path)
        Page._bufferpool = self.bufferpool

        db_file = os.path.join(path, 'db.pkl')
        if os.path.exists(db_file):
            with open(db_file, 'rb') as f:
                db_data = pickle.load(f)

            # Restore page ID counter so new pages don't collide
            Page._next_id = db_data.get('max_page_id', 1)

            for table in db_data['tables']:
                # Load all page data from individual files via bufferpool
                self._load_all_pages(table)

                # Rebuild index from data (indexes are not persisted)
                table.index = Index(table)
                self._rebuild_index(table)

                self.tables.append(table)
                self._table_map[table.name] = table

                # Reset TPS for all page ranges
                for pr in table.page_range_directory.values():
                    pr.tps = 0

            # Reset RID counter to max existing RID + 1
            max_rid = self._find_max_rid()
            if max_rid > 0:
                import lstore.query as query_module
                query_module._rid_counter = count(max_rid + 1)

    def _load_all_pages(self, table):
        """Load every page for a table from disk through the bufferpool."""
        for pr_num, pr in table.page_range_directory.items():
            for col_pages in pr.base_pages:
                for page in col_pages:
                    if page.data is None:
                        self.bufferpool.load_page(page)
            for col_pages in pr.tail_pages:
                for page in col_pages:
                    if page.data is None:
                        self.bufferpool.load_page(page)

    def _rebuild_index(self, table):
        """Scan all base records and populate the index with latest values."""
        for rid in table.page_directory:
            entry = table.page_directory[rid]
            if not entry.is_base:
                continue
            columns = table.construct_full_record(rid)
            for col_idx, value in enumerate(columns):
                if value is not None:
                    table.index.add_to_index(col_idx, value, rid)

    def _find_max_rid(self):
        """Find the maximum RID across all tables."""
        max_rid = 0
        for table in self.tables:
            for rid in table.page_directory:
                if rid > max_rid:
                    max_rid = rid
        return max_rid

    def close(self):
        """
        Persist all data to disk:
        1. Write all pages to individual files via bufferpool
        2. Pickle table metadata (with page data stripped out)
        """
        if self.path is None:
            return
        os.makedirs(self.path, exist_ok=True)

        # Wait for any background merge threads to finish
        for table in self.tables:
            if hasattr(table, 'wait_for_merge'):
                table.wait_for_merge()

        # Write all page data
        if self.bufferpool:
            self.bufferpool.write_all_pages()

        # Strip page data for pickle
        all_pages = self._collect_all_pages()
        saved_data = {}
        for page in all_pages:
            saved_data[page.page_id] = page.data
            page.data = None

        # Strip indexes (rebuilt on open)
        saved_indexes = {}
        for table in self.tables:
            saved_indexes[table.name] = table.index
            table.index = None

        db_data = {
            'tables': self.tables,
            'max_page_id': Page._next_id,
        }

        db_file = os.path.join(self.path, 'db.pkl')
        with open(db_file, 'wb') as f:
            pickle.dump(db_data, f)

        # Restore in-memory state
        for page in all_pages:
            page.data = saved_data[page.page_id]
        for table in self.tables:
            table.index = saved_indexes[table.name]

    def _collect_all_pages(self):
        """Gather every Page object across all tables."""
        pages = []
        for table in self.tables:
            for pr in table.page_range_directory.values():
                for col_pages in pr.base_pages:
                    pages.extend(col_pages)
                for col_pages in pr.tail_pages:
                    pages.extend(col_pages)
        return pages

    # Creates a new table
    def create_table(self, name, num_columns, key_index):
        # If table already exists, return it (prevents tester crash)
        if name in self._table_map:
            return self._table_map[name]

        table = Table(name, num_columns, key_index)
        self.tables.append(table)
        self._table_map[name] = table
        return table

    # Deletes the specified table
    def drop_table(self, name):
        table = self._table_map.pop(name, None)
        if table is None:
            return False
        self.tables = [t for t in self.tables if t.name != name]
        return True

    # Returns table with the passed name
    def get_table(self, name):
        return self._table_map.get(name, None)