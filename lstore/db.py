import os
from itertools import count
from lstore.table import Table, Record, PageCoord, PageDirectoryEntry, PageRange
from lstore.index import Index
from lstore.bufferpool import BufferPool
from lstore.page import Page
from lstore.lock_manager import LockManager

class Database:

    def __init__(self):
        self.tables = []
        self.table_names = {} # Simple map for quick lookup
        self.path = None
        self.bufferpool = None
        self.lock_manager = LockManager()

    def open(self, path):
        self.path = path

        Page._next_id = 1
        # Create directory if it doesn't exist

        if not os.path.exists(path):
            os.makedirs(path,exist_ok=True)

            

        self.bufferpool = BufferPool(disk_path=path)
        Page._bufferpool = self.bufferpool

        # Try to load the metadata file
        meta_file = os.path.join(path, "db.meta")
        if not os.path.exists(meta_file):
            return

        # Read all lines from the meta file
        with open(meta_file, "r") as f:
            lines = f.readlines()
            # Clean up newlines
            lines = [line.strip() for line in lines]

        self.tables = []
        self.table_names = {}

        # Loop through lines to rebuild the database state
        idx = 0
        while idx < len(lines):
            line = lines[idx]

            if not line:
                idx += 1
                continue

            # Load the global page ID counter
            if line.startswith("MAX_PAGE_ID"):
                parts = line.split("|")
                Page._next_id = int(parts[1])
            
            # Found a table block
            elif line == "TABLE_START":
                table_obj, next_idx = self._parse_table(lines, idx + 1)
                table_obj.lock_manager = self.lock_manager
                
                # Setup table basics
                self._load_all_pages(table_obj)
                table_obj.index = Index(table_obj)
                self._rebuild_index(table_obj)

                # Reset TPS for fresh start
                for pr_key in table_obj.page_range_directory:
                    pr = table_obj.page_range_directory[pr_key]
                    pr.tps = 0

                self.tables.append(table_obj)
                self.table_names[table_obj.name] = table_obj
                idx = next_idx
                continue
            
            idx += 1

        # Fix the RID counter so new records don't overlap old ones
        highest_rid = self._find_max_rid()
        if highest_rid > 0:
            import lstore.query as q
            q._rid_counter = count(highest_rid + 1)

    def close(self):
        if self.path is None:
            return

        if not os.path.exists(self.path):
            os.makedirs(self.path)

        # Make sure background merges are finished
        for t in self.tables:
            if hasattr(t, "wait_for_merge"):
                t.wait_for_merge()

        # Flush everything to disk
        if self.bufferpool:
            self.bufferpool.write_all_pages()

        # Save metadata
        meta_file = os.path.join(self.path, "db.meta")
        with open(meta_file, "w") as f:
            f.write(f"MAX_PAGE_ID|{Page._next_id}\n")
            for t in self.tables:
                self._write_table(f, t)

    def create_table(self, name, num_columns, key_index):
        if name in self.table_names:
            print(f"Error: Table {name} already exists")
            return None
        
        new_table = Table(name, num_columns, key_index)
        new_table.lock_manager = self.lock_manager
        self.tables.append(new_table)
        self.table_names[name] = new_table
        return new_table

    def drop_table(self, name):
        if name not in self.table_names:
            return False
        
        # Remove from both tracking objects
        self.table_names.pop(name)
        new_list = []
        for t in self.tables:
            if t.name != name:
                new_list.append(t)
        self.tables = new_list
        return True

    def get_table(self, name):
        if name in self.table_names:
            return self.table_names[name]
        return None

    # --- Helper methods for loading/saving ---

    def _load_all_pages(self, table):
        # Go through every page range and bring pages into bufferpool
        for pr in table.page_range_directory.values():
            # Load base pages
            for col in pr.base_pages:
                for p in col:
                    if p.data is None:
                        self.bufferpool.load_page(p)
            # Load tail pages
            for col in pr.tail_pages:
                for p in col:
                    if p.data is None:
                        self.bufferpool.load_page(p)

    def _rebuild_index(self, table):
        # Re-insert all base records into the index
        for rid, entry in table.page_directory.items():
            if entry.is_base:
                # Get data and add to index
                row_data = table.construct_full_record(rid, 0)
                for i in range(len(row_data)):
                    val = row_data[i]
                    if val is not None:
                        table.index.add_to_index(i, val, rid)

    def _find_max_rid(self):
        current_max = 0
        for t in self.tables:
            for rid in t.page_directory.keys():
                if rid > current_max:
                    current_max = rid
        return current_max

    def _write_table(self, f, t):
        f.write("TABLE_START\n")
        f.write(f"NAME|{t.name}\n")
        f.write(f"NUM_COLUMNS|{t.num_columns}\n")
        f.write(f"KEY|{t.key}\n")
        f.write(f"MERGE_THRESHOLD|{t.merge_threshold}\n")
        f.write(f"UPDATE_COUNT|{t._update_count}\n")

        # Save Page Ranges
        f.write(f"PAGE_RANGES|{len(t.page_range_directory)}\n")
        for pr_id in sorted(t.page_range_directory.keys()):
            pr = t.page_range_directory[pr_id]
            self._write_page_range(f, pr)

        # Save Page Directory
        f.write(f"PAGE_DIRECTORY|{len(t.page_directory)}\n")
        for rid in sorted(t.page_directory.keys()):
            entry = t.page_directory[rid]
            self._write_pdir_entry(f, rid, entry)

        f.write("TABLE_END\n")

    def _write_page_range(self, f, pr):
        f.write("PAGE_RANGE_START\n")
        f.write(f"PR_NUM|{pr.page_range_number}\n")
        f.write(f"NUM_RECORDS|{pr.num_records}\n")
        f.write(f"TPS|{pr.tps}\n")

        # Base Pages
        f.write(f"BASE_PAGE_COLS|{len(pr.base_pages)}\n")
        for col in pr.base_pages:
            f.write(f"COL_PAGES|{len(col)}\n")
            for p in col:
                f.write(f"PAGE|{p.page_id}|{p.num_records}|{p.current_offset}\n")

        # Tail Pages
        f.write(f"TAIL_PAGE_COLS|{len(pr.tail_pages)}\n")
        for col in pr.tail_pages:
            f.write(f"COL_PAGES|{len(col)}\n")
            for p in col:
                f.write(f"PAGE|{p.page_id}|{p.num_records}|{p.current_offset}\n")

        # Records inside the range
        f.write(f"BASE_RECORDS|{len(pr.base_records)}\n")
        for rid in sorted(pr.base_records.keys()):
            r = pr.base_records[rid]
            col_str = ",".join(str(v) if v is not None else "None" for v in r.columns)
            f.write(f"BASE_RECORD|{r.rid}|{r.key}|{col_str}\n")

        f.write(f"TAIL_RECORDS|{len(pr.tail_records)}\n")
        for rid in sorted(pr.tail_records.keys()):
            r = pr.tail_records[rid]
            col_str = ",".join(str(v) if v is not None else "None" for v in r.columns)
            f.write(f"TAIL_RECORD|{r.rid}|{r.key}|{col_str}\n")

        f.write("PAGE_RANGE_END\n")

    def _write_pdir_entry(self, f, rid, entry):
        locs = []
        for loc in entry.data_locations:
            if loc is None:
                locs.append("None")
            else:
                locs.append(f"{loc.page_number}:{loc.offset}")
        
        loc_string = ",".join(locs)
        is_base_int = 1 if entry.is_base else 0
        f.write(f"PDIR|{rid}|{entry.page_range_number}|{is_base_int}|{loc_string}\n")

    def _parse_table(self, lines, start_idx):
        # Temporary variables to store table data as we read
        t_name = ""
        t_cols = 0
        t_key = 0
        t_merge = 1000
        t_updates = 0
        t_ranges = {}
        t_pdir = {}

        i = start_idx
        while i < len(lines):
            line = lines[i]
            if line == "TABLE_END":
                t_obj = Table(t_name, t_cols, t_key)
                t_obj.merge_threshold = t_merge
                t_obj._update_count = t_updates
                t_obj.page_range_directory = t_ranges
                t_obj.page_directory = t_pdir
                # Link ranges back to this table
                for pr in t_ranges.values():
                    pr.table = t_obj
                return t_obj, i + 1

            # Simple parsing for table attributes
            if "|" in line:
                key, val = line.split("|", 1)
                if key == "NAME": t_name = val
                elif key == "NUM_COLUMNS": t_cols = int(val)
                elif key == "KEY": t_key = int(val)
                elif key == "MERGE_THRESHOLD": t_merge = int(val)
                elif key == "UPDATE_COUNT": t_updates = int(val)
                elif key == "PDIR":
                    # Handle page directory entries
                    parts = line.split("|")
                    rid = int(parts[1])
                    pr_num = int(parts[2])
                    is_base = (parts[3] == "1")
                    locs = []
                    for loc_part in parts[4].split(","):
                        if loc_part == "None":
                            locs.append(None)
                        else:
                            p_num, offset = loc_part.split(":")
                            locs.append(PageCoord(int(p_num), int(offset)))
                    t_pdir[rid] = PageDirectoryEntry(pr_num, is_base, locs)
            
            if line == "PAGE_RANGE_START":
                pr_obj, next_i = self._parse_page_range(lines, i + 1)
                t_ranges[pr_obj.page_range_number] = pr_obj
                i = next_i
                continue

            i += 1
        return None, i

    def _parse_page_range(self, lines, start_idx):
        # Create a dummy object to hold data during parsing
        pr_num = 0
        num_rec = 0
        tps = 0
        base_p = []
        tail_p = []
        base_r = {}
        tail_r = {}

        i = start_idx
        while i < len(lines):
            line = lines[i]
            if line == "PAGE_RANGE_END":
                # Using a tiny helper class or type to init PageRange
                # because PageRange expects a table object
                dummy_table = type('Obj', (object,), {'num_columns': len(base_p) - 3})
                new_pr = PageRange(dummy_table, pr_num)
                new_pr.base_pages = base_p
                new_pr.tail_pages = tail_p
                new_pr.base_records = base_r
                new_pr.tail_records = tail_r
                new_pr.num_records = num_rec
                new_pr.tps = tps
                return new_pr, i + 1

            if "|" in line:
                parts = line.split("|")
                cmd = parts[0]
                
                if cmd == "PR_NUM": pr_num = int(parts[1])
                elif cmd == "NUM_RECORDS": num_rec = int(parts[1])
                elif cmd == "TPS": tps = int(parts[1])
                elif cmd == "BASE_PAGE_COLS":
                    base_p, i = self._parse_cols(lines, i + 1, int(parts[1]))
                    continue
                elif cmd == "TAIL_PAGE_COLS":
                    tail_p, i = self._parse_cols(lines, i + 1, int(parts[1]))
                    continue
                elif cmd == "BASE_RECORD" or cmd == "TAIL_RECORD":
                    rid = int(parts[1])
                    key = int(parts[2])
                    # Parse column values
                    col_vals = []
                    for v in parts[3].split(","):
                        if v == "None": col_vals.append(None)
                        else: col_vals.append(int(v))
                    
                    rec = Record(rid, key, col_vals)
                    if cmd == "BASE_RECORD": base_r[rid] = rec
                    else: tail_r[rid] = rec

            i += 1
        return None, i

    def _parse_cols(self, lines, start_i, num_cols):
        all_cols = []
        current_i = start_i
        for _ in range(num_cols):
            line = lines[current_i]
            num_pages = int(line.split("|")[1])
            current_i += 1
            
            pages_in_col = []
            for _ in range(num_pages):
                p_line = lines[current_i]
                _, p_id, p_rec, p_off = p_line.split("|")
                new_p = Page(int(p_id))
                new_p.num_records = int(p_rec)
                new_p.current_offset = int(p_off)
                new_p.data = None
                pages_in_col.append(new_p)
                current_i += 1
            all_cols.append(pages_in_col)
        return all_cols, current_i
