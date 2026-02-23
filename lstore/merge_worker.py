import threading


class MergeWorker(threading.Thread):
    def __init__(self, table):
        super().__init__(name=f"MergeWorker-{table.name}", daemon=True)
        self.table = table

    def run(self):
        while True:
            if self.table.should_stop_merge():
                break

            page_range_number = self.table.next_merge_task(timeout=0.1)

            if page_range_number is None:
                continue

            # Execute merge only when a valid task is present
            if not self.table.should_stop_merge():
                self.table.merge_page_range(page_range_number)