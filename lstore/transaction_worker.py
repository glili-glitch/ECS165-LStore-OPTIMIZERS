import threading
import time
import random


class TransactionWorker:
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
            delay = 0.001

            while not success:
                success = transaction.run()
                if not success:
                    time.sleep(delay + random.uniform(0, delay))
                    delay = min(delay * 2, 0.05)

            self.stats.append(True)

        self.result = sum(1 for x in self.stats if x)