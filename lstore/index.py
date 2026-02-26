"""
A data structure holding indices for various columns of a table.
Key column should be indexed by default; other columns can be indexed through this object.
"""

class Index:
    # For non-PK columns: value -> set(rids)
    # For PK column:     value -> rid (single int)

    def __init__(self, table):
        self.indices = [None] * table.num_columns
        self.table = table
        self.create_index(table.key)

    def _is_pk(self, column: int) -> bool:
        return column == self.table.key

    def add_to_index(self, column, value, rid):
        if value is None:
            return True

        if self.indices[column] is None:
            self.create_index(column)

        idx = self.indices[column]

        # ✅ PK: direct mapping value -> rid
        if self._is_pk(column):
            idx[value] = rid
            return True

        # Other columns: value -> set(rids)
        s = idx.get(value)
        if s is None:
            idx[value] = {rid}
        else:
            s.add(rid)
        return True

    def remove_from_index(self, column, value, rid):
        if value is None or self.indices[column] is None:
            return False

        idx = self.indices[column]

        # ✅ PK: just remove the mapping if it matches
        if self._is_pk(column):
            existing = idx.get(value)
            if existing == rid:
                del idx[value]
                return True
            return False

        # Other columns: remove rid from the set
        s = idx.get(value)
        if not s:
            return False
        s.discard(rid)
        if len(s) == 0:
            del idx[value]
        return True

    def locate(self, column, value):
        if self.indices[column] is None:
            return None

        idx = self.indices[column]

        # PK: return [rid] or []
        if self._is_pk(column):
            rid = idx.get(value)
            return [rid] if rid is not None else []

        # Other columns: return list of rids
        s = idx.get(value)
        return list(s) if s else []

    def locate_range(self, begin, end, column):
        if self.indices[column] is None:
            return []

        idx = self.indices[column]

        # PK range queries (rare): scan keys in range
        if self._is_pk(column):
            out = []
            for v, rid in idx.items():
                if begin <= v <= end:
                    out.append(rid)
            return out

        # Other columns: union sets
        all_rids = set()
        for i in range(begin, end + 1):
            s = idx.get(i)
            if s:
                all_rids.update(s)
        return list(all_rids)

    def create_index(self, column_number):
        idx = {}
        self.indices[column_number] = idx

        # Populate from existing base records
        if hasattr(self.table, 'page_directory') and self.table.page_directory:
            for rid, entry in self.table.page_directory.items():
                if not entry.is_base:
                    continue

                columns = self.table.construct_full_record(rid)
                value = columns[column_number]
                if value is None:
                    continue

                if self._is_pk(column_number):
                    idx[value] = rid
                else:
                    s = idx.get(value)
                    if s is None:
                        idx[value] = {rid}
                    else:
                        s.add(rid)

        return True

    def drop_index(self, column_number):
        self.indices[column_number] = None