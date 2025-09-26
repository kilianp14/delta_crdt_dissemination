import csv
import glob
import os

base_dir = "../benchmarks/exp_network_full_200_push_run1"  # <-- set this
folders = [f"s{i}" for i in range(10)]

def load_rows_by_folder():
    """Load rows grouped by folder."""
    rows_by_folder = {}
    for folder in folders:
        rows = []
        for file in glob.glob(os.path.join(base_dir, folder, "version_vector_states.csv")):
            with open(file, newline="") as f:
                reader = csv.reader(f)
                try:
                    _ = next(reader)
                except StopIteration:
                    continue
                for reader_row in reader:
                    ts = int(reader_row[0])
                    values = list(map(int, reader_row[1:]))
                    rows.append((ts, values))
        rows_by_folder[folder] = rows
    return rows_by_folder

def first_occurrence_thresholds(rows, k):
    """Return list of (threshold, timestamp) for first occurrence of each value in column k."""
    seen = set()
    result = []
    for ts, values in rows:
        val = values[k]
        if val == 0:
            continue
        if val not in seen:
            seen.add(val)
            result.append((val, ts))
    return result


def process_folder_first_occurrence(data, k):
    """Process folder s{k} using first occurrence per threshold in column k."""
    other_rows = []
    for j, folder in enumerate(folders):
        if j == k:
            continue
        other_rows.extend(data[folder])

    results = []

    thresholds = first_occurrence_thresholds(data[f"s{k}"], k)

    for val, base_ts in thresholds:
        candidate_ts = [ts2 for ts2, vals2 in other_rows if vals2[k] >= val]
        min_ts = min(candidate_ts) if candidate_ts else None

        results.append({
            "from_folder": f"s{k}",
            "base_timestamp": base_ts,
            "threshold": val,
            "min_timestamp_other_folders": min_ts,
            "time_difference(us)": min_ts - base_ts
        })

    return results


if __name__ == "__main__":
    data = load_rows_by_folder()
    results = {}

    for i in range(0, 10):
        results[i] = process_folder_first_occurrence(data, i)
        for row in results[i]:
            print(row)