
import mmh3

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

class ExtendibleHashingOptimized:
    def __init__(self, bucket_capacity):
        self.global_depth = 1
        self.bucket_capacity = bucket_capacity
        self.directory = [Bucket(self.bucket_capacity, 1) for _ in range(2 ** self.global_depth)]

    def hash_key(self, key):
        return bin(hash(key) & ((1 << self.global_depth) - 1))[2:].zfill(self.global_depth)

    def get_index(self, hash_value):
        return int(hash_value[:self.global_depth], 2)

    def insert(self, record):
        hash_value = self.hash_key(record)
        #print("extendible hashed key-2: ",hash_value)

        index = self.get_index(hash_value)
        bucket = self.directory[index]

        if bucket.is_full():
            self.split_bucket(index)
            self.insert(record)  # Reinsert the record after splitting
        else:
            bucket.add_record(record)

    def split_bucket(self, index):
        old_bucket = self.directory[index]
        old_local_depth = old_bucket.local_depth

        if old_local_depth == self.global_depth:
            self.double_directory()

        old_bucket.local_depth += 1
        new_bucket = Bucket(self.bucket_capacity, old_bucket.local_depth)
        split_bit = 1 << (old_bucket.local_depth - 1)

        for i in range(len(self.directory)):
            if (i & split_bit) == split_bit and self.directory[i] == old_bucket:
                self.directory[i] = new_bucket

        records_to_redistribute = old_bucket.records[:]
        old_bucket.records.clear()

        for record in records_to_redistribute:
            self.insert(record)

    def double_directory(self):
        size = len(self.directory)
        self.directory += [None] * size

        for i in range(size):
            self.directory[i + size] = self.directory[i]

        self.global_depth += 1

    def display(self):
        print(f"Global Depth: {self.global_depth}")
        for i, bucket in enumerate(self.directory):
            print(f"Directory[{i}]: Local Depth={bucket.local_depth}, Records={bucket.records}")

class BookHashingSystem(ExtendibleHashingOptimized):
    def __init__(self, bucket_capacity):
        super().__init__(bucket_capacity)

    @staticmethod
    def murmur_hash(metadata):
        return mmh3.hash(metadata)

    @staticmethod
    def fnv_hash(isbn):
        h = 2166136261
        fnv_prime = 16777619
        for c in isbn:
            h ^= ord(c)
            h *= fnv_prime
        return h

    def hash_key(self, book):
        metadata_hash = self.murmur_hash(f"{book['title']}{book['author']}{book['publisher']}")
        salted_hash = str(metadata_hash) + "salt"
        extndbl_key = super().hash_key(salted_hash)
        #print("salted hashed key: ",salted_hash)
        #print("extendible hashed key-1: ",extndbl_key)
        return extndbl_key

# Demo Inputs
demo_inputs = [
  {"title": "Introduction to Algorithms","author": "Thomas H. Cormen","publisher": "MIT Press","year": 2009,"keywords": ["algorithms", "data structures", "computer science"]},
  {"title": "Clean Code","author": "Robert C. Martin","publisher": "Prentice Hall","year": 2008,"keywords": ["programming", "best practices", "software engineering"]}
]

# Example Usage
hashing_system = BookHashingSystem(bucket_capacity=1)

for record in demo_inputs:
    hashing_system.insert(record)

hashing_system.display()

