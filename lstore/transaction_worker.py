import threading


class TransactionWorker:
    """
    Creates a transaction worker object.
    """

    def __init__(self, transactions=None):
        self.stats = []
        if transactions is None:
            self.transactions = []
        else:
            self.transactions = transactions
        self.result = 0
        self.thread = None

    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transactions as a thread
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
        for transaction in self.transactions:
            success = transaction.run()
            self.stats.append(success)

        # stores the number of transactions that committed
        self.result = sum(1 for x in self.stats if x)