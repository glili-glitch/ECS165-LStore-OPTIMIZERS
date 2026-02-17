import os
from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = []
        # add a dict for 0(1) to lookup by name and inprove the time complex
        # key -> table name(string)
        # value -> table object
        self._table_map = {}

    # Not required for milestone1
    def open(self, path):
        # open the database at a specific directory pathy.
        self.path = path
        os.makedirs(path, exist_ok = True)
        

    def close(self):
        # close the database path.
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

        # add the table in databases and save it.
        self.tables.append(table)
        self._table_map[name] = table

        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
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
