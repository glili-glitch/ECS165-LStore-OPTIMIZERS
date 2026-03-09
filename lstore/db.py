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
        self._table_map = {}
        self.path = None
        self.bufferpool = None
        self.lock_manager = LockManager()

    def open(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)

        self.bufferpool = BufferPool(disk_path=path)
        Page._bufferpool = self.bufferpool

        db_file = os.path.join(path, "db.meta")
        if not os.path.exists(db_file):
            return

        with open(db_file, "r", encoding="utf-8") as f:
            lines = [line.rstrip("\n") for line in f]

        self.tables = []
        self._table_map = {}

        i = 0
        while i < len(lines):
            line = lines[i]

            if not line:
                i += 1
                continue

            if line.startswith("MAX_PAGE_ID|"):
                Page._next_id = int(line.split("|")[1])
                i += 1
                continue

            if line == "TABLE_START":
                table_obj, next_i = self._parse_table(lines, i + 1)
                table_obj.lock_manager = self.lock_manager
                self._load_all_pages(table_obj)
                table_obj.index = Index(table_obj)
                self._rebuild_index(table_obj)

                self.tables.append(table_obj)
                self._table_map[table_obj.name] = table_obj
                i = next_i
                continue

            i += 1

        max_rid = self._find_max_rid()
        if max_rid > 0:
            import lstore.query as query_module
            query_module._rid_counter = count(max_rid + 1)

    def close(self):
        if self.path is None:
            return

        os.makedirs(self.path, exist_ok=True)

        for table_obj in self.tables:
            if hasattr(table_obj, "wait_for_merge"):
                table_obj.wait_for_merge()

        if self.bufferpool:
            self.bufferpool.write_all_pages()

        db_file = os.path.join(self.path, "db.meta")
        with open(db_file, "w", encoding="utf-8") as f:
            f.write(f"MAX_PAGE_ID|{Page._next_id}\n")
            for table_obj in self.tables:
                self._write_table(f, table_obj)

    def create_table(self, name, num_columns, key_index):
        if name in self._table_map:
            raise ValueError(f"Table '{name}' exists.")
        table_obj = Table(name, num_columns, key_index)
        table_obj.lock_manager = self.lock_manager
        self.tables.append(table_obj)
        self._table_map[name] = table_obj
        return table_obj

    def drop_table(self, name):
        if name not in self._table_map:
            return False
        self._table_map.pop(name)
        self.tables = [t for t in self.tables if t.name != name]
        return True

    def get_table(self, name):
        return self._table_map.get(name, None)

    def _load_all_pages(self, table_obj):
        for pr in table_obj.page_range_directory.values():
            for col_pages in pr.base_pages:
                for page_obj in col_pages:
                    if page_obj.data is None:
                        self.bufferpool.load_page(page_obj)
            for col_pages in pr.tail_pages:
                for page_obj in col_pages:
                    if page_obj.data is None:
                        self.bufferpool.load_page(page_obj)

    def _rebuild_index(self, table_obj):
        for rid, entry in table_obj.page_directory.items():
            if not entry.is_base:
                continue
            columns = table_obj.construct_full_record(rid, 0)
            for col_idx, value in enumerate(columns):
                if value is not None:
                    table_obj.index.add_to_index(col_idx, value, rid)

    def _find_max_rid(self):
        max_rid = 0
        for table_obj in self.tables:
            for rid in table_obj.page_directory:
                if rid > max_rid:
                    max_rid = rid
        return max_rid

    def _write_table(self, f, table_obj):
        f.write("TABLE_START\n")
        f.write(f"NAME|{table_obj.name}\n")
        f.write(f"NUM_COLUMNS|{table_obj.num_columns}\n")
        f.write(f"KEY|{table_obj.key}\n")
        f.write(f"MERGE_THRESHOLD|{table_obj.merge_threshold}\n")
        f.write(f"UPDATE_COUNT|{table_obj._update_count}\n")

        f.write(f"PAGE_RANGES|{len(table_obj.page_range_directory)}\n")
        for pr_num in sorted(table_obj.page_range_directory.keys()):
            pr = table_obj.page_range_directory[pr_num]
            self._write_page_range(f, pr)

        f.write(f"PAGE_DIRECTORY|{len(table_obj.page_directory)}\n")
        for rid in sorted(table_obj.page_directory.keys()):
            entry = table_obj.page_directory[rid]
            self._write_page_directory_entry(f, rid, entry)

        f.write("TABLE_END\n")

    def _write_page_range(self, f, pr):
        f.write("PAGE_RANGE_START\n")
        f.write(f"PR_NUM|{pr.page_range_number}\n")
        f.write(f"NUM_RECORDS|{pr.num_records}\n")
        f.write(f"TPS|{pr.tps}\n")

        f.write(f"BASE_PAGE_COLS|{len(pr.base_pages)}\n")
        for col_pages in pr.base_pages:
            f.write(f"COL_PAGES|{len(col_pages)}\n")
            for p in col_pages:
                f.write(f"PAGE|{p.page_id}|{p.num_records}|{p.current_offset}\n")

        f.write(f"TAIL_PAGE_COLS|{len(pr.tail_pages)}\n")
        for col_pages in pr.tail_pages:
            f.write(f"COL_PAGES|{len(col_pages)}\n")
            for p in col_pages:
                f.write(f"PAGE|{p.page_id}|{p.num_records}|{p.current_offset}\n")

        f.write(f"BASE_RECORDS|{len(pr.base_records)}\n")
        for rid in sorted(pr.base_records.keys()):
            record = pr.base_records[rid]
            self._write_record(f, "BASE_RECORD", record)

        f.write(f"TAIL_RECORDS|{len(pr.tail_records)}\n")
        for rid in sorted(pr.tail_records.keys()):
            record = pr.tail_records[rid]
            self._write_record(f, "TAIL_RECORD", record)

        f.write("PAGE_RANGE_END\n")

    def _write_record(self, f, prefix, record):
        columns_str = ",".join("None" if v is None else str(v) for v in record.columns)
        f.write(f"{prefix}|{record.rid}|{record.key}|{columns_str}\n")

    def _write_page_directory_entry(self, f, rid, entry):
        loc_parts = []
        for coord in entry.data_locations:
            if coord is None:
                loc_parts.append("None")
            else:
                loc_parts.append(f"{coord.page_number}:{coord.offset}")
        loc_str = ",".join(loc_parts)
        f.write(f"PDIR|{rid}|{entry.page_range_number}|{1 if entry.is_base else 0}|{loc_str}\n")

    def _parse_table(self, lines, start_i):
        name = None
        num_columns = None
        key = None
        merge_threshold = 1000
        update_count = 0

        page_ranges = {}
        page_directory = {}

        i = start_i
        while i < len(lines):
            line = lines[i]

            if line == "TABLE_END":
                table_obj = Table(name, num_columns, key)
                table_obj.merge_threshold = merge_threshold
                table_obj._update_count = update_count
                table_obj.page_range_directory = page_ranges
                table_obj.page_directory = page_directory
                table_obj.index = Index(table_obj)

                for pr in table_obj.page_range_directory.values():
                    pr.table = table_obj

                return table_obj, i + 1

            if line.startswith("NAME|"):
                name = line.split("|", 1)[1]
            elif line.startswith("NUM_COLUMNS|"):
                num_columns = int(line.split("|")[1])
            elif line.startswith("KEY|"):
                key = int(line.split("|")[1])
            elif line.startswith("MERGE_THRESHOLD|"):
                merge_threshold = int(line.split("|")[1])
            elif line.startswith("UPDATE_COUNT|"):
                update_count = int(line.split("|")[1])
            elif line == "PAGE_RANGE_START":
                pr, next_i = self._parse_page_range(lines, i + 1)
                page_ranges[pr.page_range_number] = pr
                i = next_i
                continue
            elif line.startswith("PDIR|"):
                rid, entry = self._parse_page_directory_entry(line)
                page_directory[rid] = entry

            i += 1

        raise ValueError("Malformed metadata: missing TABLE_END")

    def _parse_page_range(self, lines, start_i):
        pr_num = None
        num_records = 0
        tps = 0
        base_pages = []
        tail_pages = []
        base_records = {}
        tail_records = {}

        i = start_i
        while i < len(lines):
            line = lines[i]

            if line == "PAGE_RANGE_END":
                dummy = type("DummyTable", (), {"num_columns": len(base_pages) - 3})()
                pr = PageRange(dummy, pr_num)
                pr.base_pages = base_pages
                pr.tail_pages = tail_pages
                pr.base_records = base_records
                pr.tail_records = tail_records
                pr.num_records = num_records
                pr.tps = tps
                return pr, i + 1

            if line.startswith("PR_NUM|"):
                pr_num = int(line.split("|")[1])
            elif line.startswith("NUM_RECORDS|"):
                num_records = int(line.split("|")[1])
            elif line.startswith("TPS|"):
                tps = int(line.split("|")[1])
            elif line.startswith("BASE_PAGE_COLS|"):
                count_cols = int(line.split("|")[1])
                base_pages, i = self._parse_page_columns(lines, i + 1, count_cols)
                continue
            elif line.startswith("TAIL_PAGE_COLS|"):
                count_cols = int(line.split("|")[1])
                tail_pages, i = self._parse_page_columns(lines, i + 1, count_cols)
                continue
            elif line.startswith("BASE_RECORDS|"):
                rec_count = int(line.split("|")[1])
                base_records, i = self._parse_records(lines, i + 1, rec_count, "BASE_RECORD")
                continue
            elif line.startswith("TAIL_RECORDS|"):
                rec_count = int(line.split("|")[1])
                tail_records, i = self._parse_records(lines, i + 1, rec_count, "TAIL_RECORD")
                continue

            i += 1

        raise ValueError("Malformed metadata: missing PAGE_RANGE_END")

    def _parse_page_columns(self, lines, start_i, count_cols):
        columns = []
        i = start_i

        for _ in range(count_cols):
            line = lines[i]
            if not line.startswith("COL_PAGES|"):
                raise ValueError("Malformed metadata: expected COL_PAGES")
            num_pages = int(line.split("|")[1])
            i += 1

            col_pages = []
            for _ in range(num_pages):
                page_line = lines[i]
                _, page_id, num_records, current_offset = page_line.split("|")
                p = Page(int(page_id))
                p.num_records = int(num_records)
                p.current_offset = int(current_offset)
                p.data = None
                col_pages.append(p)
                i += 1

            columns.append(col_pages)

        return columns, i

    def _parse_records(self, lines, start_i, rec_count, prefix):
        records = {}
        i = start_i

        for _ in range(rec_count):
            line = lines[i]
            parts = line.split("|", 3)
            if parts[0] != prefix:
                raise ValueError(f"Malformed metadata: expected {prefix}")

            rid = int(parts[1])
            key = int(parts[2])
            columns = self._parse_columns(parts[3])
            records[rid] = Record(rid, key, columns)
            i += 1

        return records, i

    def _parse_columns(self, s):
        if s == "":
            return []
        values = []
        for part in s.split(","):
            if part == "None":
                values.append(None)
            else:
                values.append(int(part))
        return values

    def _parse_page_directory_entry(self, line):
        parts = line.split("|")
        rid = int(parts[1])
        page_range_number = int(parts[2])
        is_base = parts[3] == "1"
        loc_str = parts[4]

        data_locations = []
        for token in loc_str.split(","):
            if token == "None":
                data_locations.append(None)
            else:
                page_number, offset = token.split(":")
                data_locations.append(PageCoord(int(page_number), int(offset)))

        return rid, PageDirectoryEntry(page_range_number, is_base, data_locations)