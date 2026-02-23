"""

Secondary indexing for L-Store tables.
"""


class Index:
    def __init__(self, table):
        self.table = table
        # Key column should be indexed by default (commonly column 0).
        # If your table defines key column differently, change 0 to that column index.
        self.create_index(0)
        pass


    def add_to_index(self, column_number, value, rid):
        if value is None:
            return True
        if self.indices[column_number] is None:
            return True
        index_map = self.indices[column_number]
        bucket = index_map.get(value)
        if bucket is None:
            index_map[value] = {rid}
        else:
            bucket.add(rid)
        return True

    def remove_from_index(self, column_number, value, rid):
        if value is None:
            return True
        index_map = self.indices[column_number]
        if index_map is None:
            return True
        bucket = index_map.get(value)
        if bucket is None:
            return True
        bucket.discard(rid)
        if not bucket:
            index_map.pop(value, None)
        return True

    def locate(self, column, value):
        index_map = self.indices[column]
        if index_map is None:
            return []
        return list(index_map.get(value, set()))

    def locate_range(self, begin, end, column):
        index_map = self.indices[column]
        if index_map is None:
            rids = set()
            for rid in self.table.iter_base_rids(include_deleted=False):
                value = self.table.get_column_value(rid, column)
                if value is None:
                    continue
                if begin <= value <= end:
                    rids.add(rid)
            return list(rids)

        rids = set()
        for value, bucket in index_map.items():
            if begin <= value <= end:
                rids.update(bucket)
        return list(rids)

    def create_index(self, column_number):
        if self.indices[column_number] is not None:
            return True
        index_map = {}
        self.indices[column_number] = index_map
        for rid in self.table.iter_base_rids(include_deleted=False):
            value = self.table.get_column_value(rid, column_number)
            if value is None:
                continue
            bucket = index_map.get(value)
            if bucket is None:
                index_map[value] = {rid}
            else:
                bucket.add(rid)
        return True

    def drop_index(self, column_number):
        self.indices[column_number] = None
        return True

