import json
from src.hashing import BookHashingSystem

def load_data(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

if __name__ == "__main__":
    # Initialize the hashing system
    hashing_system = BookHashingSystem(bucket_capacity=3)

    # Load demo inputs
    data = load_data("data/demo_inputs.json")


    # Insert records
    for record in data:
        hashing_system.insert(record)

    # Display the hashing system
    hashing_system.display()
