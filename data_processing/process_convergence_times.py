import csv
import glob
import os
from collections import defaultdict

def load_convergence_data(base_dir):
    """Load rows grouped by folder s0-s9."""
    rows_by_folder = {}
    for folder in [f"s{i}" for i in range(10)]:
        rows = []
        for file in glob.glob(os.path.join(base_dir, folder, "version_vector_states.csv")):
            with open(file, newline="") as f:
                reader = csv.reader(f)
                try:
                    _ = next(reader)  # skip header
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
    results = []

    thresholds = first_occurrence_thresholds(data[f"s{k}"], k)

    for val, base_ts in thresholds:
        max_ts = 0
        for folder, rows in data.items():
            if folder == f"s{k}":
                continue
            candidate_ts = [ts2 for ts2, vals2 in rows if vals2[k] >= val]
            min_ts = min(candidate_ts) if candidate_ts else None
            if min_ts is None:  # did not converge
                max_ts = None
                break
            if min_ts > max_ts:
                max_ts = min_ts
        if max_ts is not None:
            results.append(max_ts - base_ts)  # just store the time difference

    return results

def parse_folder_name(folder):
    """
    Parse folder name like:
    exp_networkring_100_pull_one_run1
    -> network=networkring, update_freq=100, strategy=pull, fanout=one, run=1
    """
    base = os.path.basename(folder)
    parts = base.split("_")
    return {
        "network": parts[1],
        "update_freq": int(parts[2]),
        "strategy": parts[3],
        "fanout": parts[4],
        "run": int(parts[-1].replace("run", ""))
    }

def aggregate_convergence_times(base_pattern):
    run_folders = sorted(glob.glob(base_pattern + "*"))
    if not run_folders:
        print("No matching folders found.")
        return None

    # dict[(network, update_freq, strategy, fanout)] -> list of time differences
    all_times = defaultdict(list)

    for run_folder in run_folders:
        parsed = parse_folder_name(run_folder)
        data = load_convergence_data(run_folder)
        for i in range(10):
            times = process_folder_first_occurrence(data, i)
            all_times[(parsed["network"], parsed["update_freq"], parsed["strategy"], parsed["fanout"])].extend(times)

    # compute averages per combination
    avg_times = []
    for key, times in all_times.items():
        network, update_freq, strategy, fanout = key
        avg_time = sum(times) / len(times) if times else None
        avg_times.append({
            "network": network,
            "update_freq": update_freq,
            "strategy": strategy,
            "fanout": fanout,
            "avg_convergence_time_us": avg_time
        })

    return avg_times

def save_to_csv(data, out_file="results/convergence_times_avg.csv"):
    """Save list of dicts to CSV."""
    if not data:
        print("No data to save.")
        return

    # Ensure the directory exists
    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    keys = data[0].keys()
    with open(out_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    print(f"✅ Convergence times averages saved to {out_file}")

if __name__ == "__main__":
    base_pattern = "../benchmarks/exp_network"
    avg_convergence = aggregate_convergence_times(base_pattern)
    save_to_csv(avg_convergence)
