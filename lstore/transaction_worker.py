import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        self.stats = []
        if transactions is None:
            self.transactions = []
        else:
            self.transactions = transactions
        self.result = 0
        self.thread = None
        pass

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        if self.thread:
            self.thread.join()


    def __run(self):
        for i, transaction in enumerate(self.transactions):
            while True:
                success = transaction.run()
                if success:
                    self.stats.append(True)
                    break
            
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

