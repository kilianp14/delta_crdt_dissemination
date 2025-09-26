import csv
import os

def aggregate_received_messages(base_folder):
    folders = [f"s{i}" for i in range(10)]

    total_bytes = 0
    redundant_bytes = 0
    redundant_count = 0
    total_messages = 0

    for folder in folders:
        file_path = os.path.join(base_folder, folder, "received_message_sizes.csv")
        if not os.path.exists(file_path):
            continue

        with open(file_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                size = int(row["size_bytes"])
                redundant = row["redundant"].strip().lower() == "true"
                total_bytes += size
                total_messages += 1

                if redundant:
                    redundant_bytes += size
                    redundant_count += 1

    redundant_percentage = (redundant_count / total_messages * 100) if total_messages else 0

    return {
        "total_bytes": total_bytes,
        "redundant_bytes": redundant_bytes,
        "redundant_count": redundant_count,
        "redundant_percentage": redundant_percentage
    }

if __name__ == "__main__":
    base_folder = "../benchmarks/exp_network_full_200_push_run1"
    stats = aggregate_received_messages(base_folder)

    print("Received Message Sizes Aggregation:")
    print(f"Total bytes: {stats['total_bytes']}")
    print(f"Total redundant bytes: {stats['redundant_bytes']}")
    print(f"Number of redundant messages: {stats['redundant_count']}")
    print(f"Percentage of redundant messages: {stats['redundant_percentage']:.2f}%")
