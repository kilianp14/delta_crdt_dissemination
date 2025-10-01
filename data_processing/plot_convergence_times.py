import pandas as pd
import matplotlib.pyplot as plt
import os

def plot_convergence_bar(csv_file="results/convergence_times_avg.csv"):
    # Load the CSV
    df = pd.read_csv(csv_file)

    # Ensure output folder exists
    outdir = "plots/convergence_plots"
    os.makedirs(outdir, exist_ok=True)

    networks = df["network"].unique()

    for net in networks:
        sub = df[df["network"] == net]

        # Create labels for each strategy+fanout
        labels = sub.apply(lambda row: f"{row['strategy']}-{row['fanout']}", axis=1)
        values = sub["avg_convergence_time_us"]

        plt.figure(figsize=(12, 6))
        plt.bar(labels, values, color=plt.cm.tab20.colors[:len(labels)])
        plt.xticks(rotation=45, ha="right")
        plt.ylabel("Average Convergence Time (µs)")
        plt.title(f"Average Convergence Time per Strategy/Fanout ({net})")
        plt.tight_layout()
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.savefig(os.path.join(outdir, f"{net}_convergence_bar.png"))
        plt.close()

    print(f"✅ Convergence bar plots saved in folder: {outdir}")

if __name__ == "__main__":
    plot_convergence_bar()
