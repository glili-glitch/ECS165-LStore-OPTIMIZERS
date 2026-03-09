import uuid

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.transaction_id = uuid.uuid4()
        self.held_locks = set() # Set of (table, rid) tuples
        self.rollback_log = [] # List of (table, rid, old_indirection, old_schema)
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, table, args in self.queries:
            result = query(*args, transaction=self)
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        # Rollback changes to RIDs and metadata
        for table, rid, old_indirection, old_schema in reversed(self.rollback_log):
            with table.directory_lock:
                if rid in table.page_directory:
                    pass 
        
        # Release all locks
        for table, rid in self.held_locks:
            if table.lock_manager:
                table.lock_manager.release_locks(self.transaction_id, [rid])
        
        self.held_locks.clear()
        self.rollback_log.clear()
        return False

    
    def commit(self):
        # Release all locks
        for table, rid in self.held_locks:
            if table.lock_manager:
                table.lock_manager.release_locks(self.transaction_id, [rid])
        
        self.held_locks.clear()
        self.rollback_log.clear()
        return True

