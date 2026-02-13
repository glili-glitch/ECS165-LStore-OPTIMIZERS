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
        self.create_index(0)
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
            rids_set.discard(rid)
            if rids_set == set():
                rids_set = None
            idx[value] = rids_set
        else:
            return False
        return True

    def locate(self, column, value):
        if self.indices[column] is None:
            return None
        idx = self.indices[column]
        rids = idx.get(value, set())
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
        return True
    
    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None