import threading


class Index:
    # every index is a dictionary that maps a column value to a set of RIDs
    def __init__(self, table):
        self.indices = [None] * table.num_columns
        self.table = table
        self.lock = threading.Lock()

        # Primary key column indexed by default
        self.create_index(table.key)

    def add_to_index(self, column, value, rid):
        with self.lock:
            if self.indices[column] is None:
                self.create_index_unlocked(column)

            idx = self.indices[column]

            if value in idx:
                rids_set = idx[value]
                if rids_set is None:
                    rids_set = set()
                rids_set.add(rid)
                idx[value] = rids_set
            else:
                idx[value] = {rid}

            return True

    def remove_from_index(self, column, value, rid):
        with self.lock:
            if self.indices[column] is None:
                return False

            idx = self.indices[column]

            if value in idx:
                rids_set = idx[value]
                if rids_set is not None:
                    rids_set.discard(rid)
                    if len(rids_set) == 0:
                        del idx[value]
                    else:
                        idx[value] = rids_set
            else:
                return False

            return True

    def locate(self, column, value):
        with self.lock:
            if self.indices[column] is None:
                return []

            idx = self.indices[column]
            rids = idx.get(value, None)

            if rids is None:
                return []

            return list(rids)

    def locate_range(self, begin, end, column):
        with self.lock:
            all_rids = set()
            for i in range(begin, end + 1):
                res = self.locate_unlocked(column, i)
                if res:
                    all_rids = all_rids.union(res)
            return list(all_rids)

    def locate_unlocked(self, column, value):
        if self.indices[column] is None:
            return []

        idx = self.indices[column]
        rids = idx.get(value, None)

        if rids is None:
            return []

        return list(rids)

    def create_index(self, column_number):
        with self.lock:
            return self.create_index_unlocked(column_number)

    def create_index_unlocked(self, column_number):
        idx = {}
        self.indices[column_number] = idx

        # Build index from existing base records only
        if hasattr(self.table, 'page_directory') and self.table.page_directory:
            for rid, entry in self.table.page_directory.items():
                if not entry.is_base:
                    continue
                if hasattr(entry, "is_deleted") and entry.is_deleted:
                    continue

                columns = self.table.construct_full_record(rid)
                if columns is None or len(columns) <= column_number:
                    continue

                value = columns[column_number]
                if value is not None:
                    if value in idx:
                        idx[value].add(rid)
                    else:
                        idx[value] = {rid}

        return True

    def drop_index(self, column_number):
        with self.lock:
            self.indices[column_number] = None