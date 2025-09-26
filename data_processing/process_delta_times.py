import csv
import os

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

if __name__ == "__main__":
    base_folder = "../benchmarks/exp_network_full_200_pull_run1"
    per_folder, total_get, total_merge = aggregate_sums(base_folder)

    print("Per-folder sums:")
    for folder, sums in per_folder.items():
        print(f"{folder}: get={sums['get']}, merge={sums['merge']}")

    print("\nTotal sums across all folders:")
    print(f"get_delta_times.csv total: {total_get}")
    print(f"merge_delta_times.csv total: {total_merge}")
