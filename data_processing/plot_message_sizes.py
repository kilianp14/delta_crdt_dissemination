import pandas as pd
import matplotlib.pyplot as plt
import os

def plot_received_messages(csv_file="results/received_messages_totals_avg.csv"):
    df = pd.read_csv(csv_file)

    # Ensure output folder exists
    outdir = "plots/message_sizes"
    os.makedirs(outdir, exist_ok=True)

    networks = df["network"].unique()

    # Generate a distinct color for each strategy+dissemination combination
    all_groups = df.groupby(["strategy", "dissemination"]).size().index.tolist()
    cmap = plt.get_cmap("tab20")  # 20 distinctive colors
    color_map = {group: cmap(i % 20) for i, group in enumerate(all_groups)}

    for net in networks:
        sub = df[df["network"] == net].sort_values("fanout")  # fanout = update frequency

        # --- 1. Redundant percentage vs. update frequency ---
        fig, ax = plt.subplots(figsize=(8, 5))
        for (strategy, dissemination), group in sub.groupby(["strategy", "dissemination"]):
            color = color_map[(strategy, dissemination)]
            ax.plot(group["fanout"], group["avg_redundant_percentage"],
                    marker="o", label=f"{strategy}-{dissemination}", color=color)
        ax.set_title(f"Redundant % vs Update Frequency ({net})")
        ax.set_xlabel("Update Frequency")
        ax.set_ylabel("Redundant %")
        ax.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, f"{net}_redundant_percentage.png"))
        plt.close()

        # --- 2. Total vs Redundant Bytes (grouped bar chart by update frequency) ---
        fig, ax = plt.subplots(figsize=(10, 6))
        pivot = sub.pivot_table(
            index="fanout",
            columns=["strategy", "dissemination"],
            values=["avg_total_bytes", "avg_redundant_bytes"]
        )
        pivot_cols = pivot.columns.tolist()
        n_bars = len(pivot_cols)
        bar_width = 0.8 / n_bars

        for i, col in enumerate(pivot_cols):
            metric, strategy, dissemination = col
            values = pivot[col].values
            x = [r + i*bar_width for r in range(len(pivot))]
            ax.bar(
                x=x,
                height=values,
                width=bar_width,
                label=f"{metric}-{strategy}-{dissemination}",
                color=color_map[(strategy, dissemination)]
            )

        ax.set_xticks([r + bar_width*(n_bars/2) for r in range(len(pivot))])
        ax.set_xticklabels(pivot.index)
        ax.set_xlabel("Update Frequency")
        ax.set_ylabel("Bytes")
        ax.set_title(f"Total vs Redundant Bytes ({net})")
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title="Metric / Strategy-Dissemination")
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, f"{net}_bytes.png"))
        plt.close()

        # --- 3. Redundant Count vs. update frequency ---
        fig, ax = plt.subplots(figsize=(8, 5))
        for (strategy, dissemination), group in sub.groupby(["strategy", "dissemination"]):
            color = color_map[(strategy, dissemination)]
            ax.plot(group["fanout"], group["avg_redundant_count"],
                    marker="s", label=f"{strategy}-{dissemination}", color=color)
        ax.set_title(f"Redundant Count vs Update Frequency ({net})")
        ax.set_xlabel("Update Frequency")
        ax.set_ylabel("Count")
        ax.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, f"{net}_redundant_count.png"))
        plt.close()

    print(f"✅ Plots saved in folder: {outdir}")

if __name__ == "__main__":
    plot_received_messages()
