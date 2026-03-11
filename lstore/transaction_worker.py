import threading
import time


class TransactionWorker:
    """
    Creates a transaction worker object.
    """

    def __init__(self, transactions=None):
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0
        self.thread = None

    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()

    def join(self):
        if self.thread is not None:
            self.thread.join()

    def __run(self):
        self.stats = []

        for transaction in self.transactions:
            success = False

            # Retry until commit succeeds
            while not success:
                success = transaction.run()
                if not success:
                    time.sleep(0.001)

            self.stats.append(success)

        self.result = sum(1 for x in self.stats if x)