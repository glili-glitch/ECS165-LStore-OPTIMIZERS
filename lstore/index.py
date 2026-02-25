"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    # every index is a dictionary that maps a column value to a set of RIDs 
    def __init__(self, table):
        self.indices = [None] *  table.num_columns
        self.table = table
        # Key column should be indexed by default (commonly column 0).
        # If your table defines key column differently, change 0 to that column index.
        self.create_index(table.key)
        pass


    def add_to_index(self, column, value, rid):
        if self.indices[column] is None:
            self.create_index(column)
        idx = self.indices[column]
        if value in idx: 
            rids_set = idx[value]
            if rids_set is None:
                rids_set = set()
            rids_set.add(rid)
            idx[value] = rids_set
        else:
            idx[value] = set([rid])
        return True

    def remove_from_index(self, column, value, rid):
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
        if self.indices[column] is None:
            return None
        idx = self.indices[column]
        rids = idx.get(value, None)
        if rids is None:
            return []
        return list(rids)


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        all_rids = set()
        for i in range(begin, end + 1):
            all_rids = all_rids.union(self.locate(column, i))
        return list(all_rids)

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        idx = {}
        self.indices[column_number] = idx

        # Populate index from existing data (supports creating indexes after inserts)
        if hasattr(self.table, 'page_directory') and self.table.page_directory:
            for rid, entry in self.table.page_directory.items():
                if not entry.is_base:
                    continue
                # Get the latest value for this column
                columns = self.table.construct_full_record(rid)
                value = columns[column_number]
                if value is not None:
                    if value in idx:
                        idx[value].add(rid)
                    else:
                        idx[value] = {rid}

        return True
    
    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None