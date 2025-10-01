import csv
import os
import re
from collections import defaultdict

def sum_csv_column(file_path, column_name="time_micros"):
    """Sum the specified column in a CSV file."""
    total = 0
    if not os.path.exists(file_path):
        return 0
    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += int(row[column_name])
    return total

def aggregate_sums(base_folder):
    """Aggregate sums of get and merge for one run folder."""
    folders = [f"s{i}" for i in range(10)]
    total_get = 0
    total_merge = 0
    per_folder_sums = {}

    for folder in folders:
        folder_path = os.path.join(base_folder, folder)
        get_file = os.path.join(folder_path, "get_delta_times.csv")
        merge_file = os.path.join(folder_path, "merge_delta_times.csv")

        get_sum = sum_csv_column(get_file)
        merge_sum = sum_csv_column(merge_file)

        per_folder_sums[folder] = {"get": get_sum, "merge": merge_sum}
        total_get += get_sum
        total_merge += merge_sum

    return per_folder_sums, total_get, total_merge

def parse_folder_name(folder_name):
    """
    Parse folder names like:
    exp_networkring_100_pushpull_full_run3
    -> ("networkring", "100", "pushpull", "full", "3")
    """
    m = re.match(r"exp_(.+)_(\d+)_(.+)_(.+)_run(\d+)", folder_name)
    if not m:
        return None
    return m.groups()  # topology, size, strategy, fanout, run

def aggregate_all(base_dir="../benchmarks"):
    """Group by parameter combinations and average across runs."""
    all_folders = [f for f in os.listdir(base_dir) if f.startswith("exp_")]
    groups = defaultdict(list)

    # group runs by (topology, size, strategy, fanout)
    for folder in all_folders:
        parsed = parse_folder_name(folder)
        if not parsed:
            continue
        topology, size, strategy, fanout, run = parsed
        key = (topology, size, strategy, fanout)
        groups[key].append(os.path.join(base_dir, folder))

    results = {}

    for key, run_folders in groups.items():
        total_get_all = 0
        total_merge_all = 0
        num_runs = len(run_folders)

        avg_per_folder = {f"s{i}": {"get": 0, "merge": 0} for i in range(10)}

        for run in run_folders:
            per_folder, total_get, total_merge = aggregate_sums(run)
            total_get_all += total_get
            total_merge_all += total_merge

            for folder, sums in per_folder.items():
                avg_per_folder[folder]["get"] += sums["get"]
                avg_per_folder[folder]["merge"] += sums["merge"]

        # average across runs
        for folder in avg_per_folder:
            avg_per_folder[folder]["get"] //= num_runs
            avg_per_folder[folder]["merge"] //= num_runs

        avg_total_get = total_get_all // num_runs
        avg_total_merge = total_merge_all // num_runs

        results[key] = (avg_per_folder, avg_total_get, avg_total_merge, num_runs)

    return results

def save_results(results, output_dir="./results"):
    os.makedirs(output_dir, exist_ok=True)

    # Write totals per combination
    totals_file = os.path.join(output_dir, "totals.csv")
    with open(totals_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["topology", "size", "strategy", "fanout", "runs", "avg_get_total", "avg_merge_total"])
        for (topology, size, strategy, fanout), (_, avg_total_get, avg_total_merge, num_runs) in results.items():
            writer.writerow([topology, size, strategy, fanout, num_runs, avg_total_get, avg_total_merge])

    # Write per-folder averages
    per_folder_file = os.path.join(output_dir, "per_folder.csv")
    with open(per_folder_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["topology", "size", "strategy", "fanout", "runs", "folder", "avg_get", "avg_merge"])
        for (topology, size, strategy, fanout), (avg_per_folder, _, _, num_runs) in results.items():
            for folder, sums in avg_per_folder.items():
                writer.writerow([topology, size, strategy, fanout, num_runs, folder, sums["get"], sums["merge"]])

    print(f"Saved results to {totals_file} and {per_folder_file}")


if __name__ == "__main__":
    base_dir = "../benchmarks"
    results = aggregate_all(base_dir)

    for (topology, size, strategy, fanout), (avg_per_folder, avg_total_get, avg_total_merge, num_runs) in results.items():
        print(f"=== Combination: {topology}, size={size}, strategy={strategy}, fanout={fanout} (runs={num_runs}) ===\n")

        print("Per-folder averages:")
        for folder, sums in avg_per_folder.items():
            print(f"{folder}: get={sums['get']}, merge={sums['merge']}")

        print("\nAverage totals across runs:")
        print(f"get_delta_times.csv average: {avg_total_get}")
        print(f"merge_delta_times.csv average: {avg_total_merge}")
        print("\n")

    save_results(results)
