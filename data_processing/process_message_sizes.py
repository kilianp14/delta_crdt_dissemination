import csv
import os
import re
import pandas as pd
from collections import defaultdict

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

def parse_folder_name(folder_name):
    """
    Example: exp_networkring_100_pushpull_full_run3
             -> ('networkring', 100, 'pushpull', 'full')
    """
    match = re.match(r"exp_([^_]+)_(\d+)_(\w+)_(\w+)_run\d+", folder_name)
    if not match:
        return None
    return match.groups()

if __name__ == "__main__":
    base_path = "../benchmarks"
    grouped_results = defaultdict(list)

    for folder in os.listdir(base_path):
        folder_path = os.path.join(base_path, folder)
        if not os.path.isdir(folder_path):
            continue

        parsed = parse_folder_name(folder)
        if not parsed:
            continue

        network, fanout, strategy, dissemination = parsed
        stats = aggregate_received_messages(folder_path)

        key = (network, int(fanout), strategy, dissemination)
        grouped_results[key].append(stats)

    # Compute averages across runs
    averaged_results = []
    for (network, fanout, strategy, dissemination), runs in grouped_results.items():
        avg_total_bytes = sum(r["total_bytes"] for r in runs) / len(runs)
        avg_redundant_bytes = sum(r["redundant_bytes"] for r in runs) / len(runs)
        avg_redundant_count = sum(r["redundant_count"] for r in runs) / len(runs)
        avg_redundant_percentage = sum(r["redundant_percentage"] for r in runs) / len(runs)

        averaged_results.append({
            "network": network,
            "fanout": fanout,
            "strategy": strategy,
            "dissemination": dissemination,
            "avg_total_bytes": avg_total_bytes,
            "avg_redundant_bytes": avg_redundant_bytes,
            "avg_redundant_count": avg_redundant_count,
            "avg_redundant_percentage": avg_redundant_percentage
        })

    # Save combined averaged results
    df = pd.DataFrame(averaged_results)
    os.makedirs('results', exist_ok=True)
    df.to_csv("results/received_messages_totals_avg.csv", index=False)

    print("Aggregation complete. Results saved to received_messages_totals_avg.csv")
    print(df.head())
