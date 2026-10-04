import pandas as pd
import matplotlib.pyplot as plt
import os

def plot_totals(csv_file, output_dir="./plots/delta_times"):
    os.makedirs(output_dir, exist_ok=True)
    df = pd.read_csv(csv_file)

    # Build a "group label" for strategy+fanout
    df["group"] = df["strategy"] + "_" + df["fanout"]

    # Plot one figure per topology
    for topology, group_df in df.groupby("topology"):
        plt.figure(figsize=(10, 6))

        # Pivot so that size is x-axis and each group is a bar color
        pivot = group_df.pivot_table(
            index="size", columns="group", values="avg_get_total", aggfunc="mean"
        )

        pivot.plot(kind="bar", ax=plt.gca())
        plt.title(f"Average Get Total – {topology}")
        plt.ylabel("Average Get Total")
        plt.xlabel("Size")
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{topology}_avg_get_total.png"))
        plt.close()

        # Same for merge totals
        plt.figure(figsize=(10, 6))
        pivot = group_df.pivot_table(
            index="size", columns="group", values="avg_merge_total", aggfunc="mean"
        )
        pivot.plot(kind="bar", ax=plt.gca())
        plt.title(f"Average Merge Total – {topology}")
        plt.ylabel("Average Merge Total")
        plt.xlabel("Size")
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{topology}_avg_merge_total.png"))
        plt.close()

    # Also: scatter plot comparing get vs merge per topology
    for topology, group_df in df.groupby("topology"):
        plt.figure(figsize=(8, 6))
        plt.scatter(group_df["avg_get_total"], group_df["avg_merge_total"])

        for i, row in group_df.iterrows():
            label = f"{row['size']}_{row['group']}"
            plt.text(row["avg_get_total"], row["avg_merge_total"], label, fontsize=8)

        plt.xlabel("Average Get Total")
        plt.ylabel("Average Merge Total")
        plt.title(f"Get vs Merge Totals – {topology}")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{topology}_get_vs_merge.png"))
        plt.close()

    print(f"Plots saved in {output_dir}")

if __name__ == "__main__":
    plot_totals("./results/totals.csv")
