class Bucket:
    def __init__(self, capacity, local_depth):
        self.capacity = capacity
        self.local_depth = local_depth
        self.records = []

    def is_full(self):
        return len(self.records) >= self.capacity

    def add_record(self, record):
        if not self.is_full():
            self.records.append(record)
        else:
            raise Exception("Bucket is full!")

    def remove_record(self, record):
        self.records.remove(record)

    def __repr__(self):
        return f"Bucket(depth={self.local_depth}, records={self.records})"
