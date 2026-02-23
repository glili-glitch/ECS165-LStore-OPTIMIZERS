import os
import json
from lstore.page import Page,PAGE_SIZE
from lstore.table import Table, PageCoord, PageDirectoryEntry

class Database():

    def __init__(self):
        self.tables = []
        # add a dict for 0(1) to lookup by name and inprove the time complex
        # key -> table name(string)
        # value -> table object
        self._table_map = {}
        self.path = None

    # helper function for disk path
    def _table_dir(self, table_name: str):
        return os.path.join(self.path, table_name)

    def _ensure_table_dirs(self, table_name: str):
        os.makedirs(os.path.join(self._table_dir(table_name), "base"), exist_ok = True)
        os.makedirs(os.path.join(self._table_dir(table_name), "tail"), exist_ok = True)

    def _meta_file(self, table_name: str):
        return os.path.join(self._table_dir(table_name), "meta.json")
    
    def _page_file(self, table_name: str, is_tail: bool ,page_range_id: int, cid: int, pid: int):
        folder = "tail" if is_tail else 'base'
        fname = f"pr_{page_range_id}_col_{cid}_pg_{pid}.bin"
        return os.path.join(self._table_dir(table_name), folder, fname)

    def _write_page_bytes(self, filepath: str, page_bytes: bytes):
        if len(page_bytes) != PAGE_SIZE:
            raise ValueError (f"Not expected page size")
        os.makedirs(os.path.dirname(filepath), exist_ok = True)
        with open(filepath, "wb") as files:
            files.write(page_bytes)

    def _read_page_bytes(self, filepath: str):
        with open(filepath, "rb") as files:
            pagebytes = files.read()
        if len(pagebytes) != PAGE_SIZE:
            raise ValueError(f"wrong page file!")
        return pagebytes

    def _read_page_or_empty(self, filepath):
        # check if the disk path if is empty then return
        if not os.path.exists(filepath):
            return Page()
        return Page.from_bytes(self._read_page_bytes(filepath))


    
    # Not required for milestone1
    def open(self, path):
        # open the database at a specific directory pathy.
        self.path = path
        os.makedirs(path, exist_ok = True)

        self.tables.clear()
        self._table_map.clear()

        for table_name in os.listdir(path):
            table_dir = os.path.join(path, table_name)
            meta_path = os.path.join(table_dir, "meta.json")
            if (not os.path.isdir(table_dir)) or (not os.path.exists(meta_path)):
                continue

            with open(meta_path, "r", encoding = "utf-8") as file:
                meta = json.load(file)

            table = Table(meta["name"], meta["num_columns"], meta["key"])
            total_columns = table.num_columns + 3 # hidden cols

            # Avodi Rid collison we have to restore next RID
            table.next_rid = meta.get("next_rid", 1)

            # saving page
            for prid_str, pr_meta in meta.get("page_ranges", {}).items():
                page_range_id = int(prid_str)
                table.add_page_range(page_range_id)
                pr = table.page_range_directory[page_range_id]

                base_counts = pr_meta["base_pages_per_col"]
                tail_counts = pr_meta["tail_pages_per_col"]

                # base page
                pr.base_pages = []
                for cid in range(total_columns):
                    col_pages = []
                    for pid in range(base_counts[cid]):
                        pfile = self._page_file(table.name, False, page_range_id, cid, pid)
                        col_pages.append(self._read_page_or_empty(pfile))
                    pr.base_pages.append(col_pages)

                # tail pages
                pr.tail_pages = []
                for cid in range(total_columns):
                    col_pages = []
                    for pid in range(tail_counts[cid]):
                        pfile = self._page_file(table.name, True, page_range_id, cid, pid)
                        col_pages.append(self._read_page_or_empty(pfile))
                    pr.tail_pages.append(col_pages)

            # restore page_directory
            table.page_directory = {}
            for rid_str, entry in meta.get("page_directory", {}).items():
                rid = int(rid_str)
                coords = []
                for c in entry["data_locations"]:
                    if c is None:
                        coords.append(None)
                    else:
                        coords.append(PageCoord(c[0], c[1]))

                table.page_directory[rid] = PageDirectoryEntry(
                    entry["page_range_number"],
                    entry["is_base"],
                    coords
                )

            self.tables.append(table)
            self._table_map[table.name] = table
    
        

    def close(self):
        # pages and  meta.json flush to disk.
        if self.path is None:
            return

        for table in self.tables:
            self._ensure_table_dirs(table.name)
            total_col = table.num_columns + 3

            page_ranges_meta = {}

            # base/tail pages flush
            for page_range_id, pr in table.page_range_directory.items():
                base_counts = []
                tail_counts = []

                # base
                for cid in range(total_col):
                    base_counts.append(len(pr.base_pages[cid]))
                    for pid, pg in enumerate(pr.base_pages[cid]):
                        pfile = self._page_file(table.name, False, page_range_id, cid, pid)
                        self._write_page_bytes(pfile, pg.to_bytes())

                # tail
                for cid in range(total_col):
                    tail_counts.append(len(pr.tail_pages[cid]))
                    for pid, pg in enumerate(pr.tail_pages[cid]):
                        pfile = self._page_file(table.name, True, page_range_id, cid, pid)
                        self._write_page_bytes(pfile, pg.to_bytes())

                page_ranges_meta[str(page_range_id)] = {
                    "base_pages_per_col": base_counts,
                    "tail_pages_per_col": tail_counts
                }

            # page_directory
            page_dir_meta = {
                str(rid): {
                    "page_range_number": entry.page_range_number,
                    "is_base": entry.is_base,
                    "data_locations": [
                        [coord.page_number, coord.offset] if coord is not None else None
                        for coord in entry.data_locations
                    ]
                }
                for rid, entry in table.page_directory.items()
            }

            meta = {
                "name": table.name,
                "num_columns": table.num_columns,
                "key": table.key,
                "page_ranges": page_ranges_meta,
                "page_directory": page_dir_meta,
                "next_rid": getattr(table, "next_rid", 1)
            }

            with open(self._meta_file(table.name), "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)

        # memory clear
        self.tables.clear()
        self._table_map.clear()


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # for safety we are going to lookup for search name if it's already exists
        if name in self._table_map:
            raise ValueError(f"Table '{name}' exists.")

        table = Table(name, num_columns, key_index)

        # for Milestone 2 keep directory
        self._ensure_table_dirs(name)
    
        # for Milestone 2 matian RID persistence
        table.next_rid = 1

        # add the table in databases and save it.
        self.tables.append(table)
        self._table_map[name] = table

        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        # delete table entry
        if name in self._table_map:
            del self._table_map[name]

        # Iterate throught the list of tables
        for i, table in enumerate(self.tables):
            # check if the table found or not
            if table.name == name:
                # pop table from the stock
                self.tables.pop(i)
                return True
        # if the table not match then 
        return False
                

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        # 0(1) averageg lookup to improve time complexity
        return self._table_map.get(name,None)
