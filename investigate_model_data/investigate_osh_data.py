"""This script investigates the OSH restoration limit data, focusing on discharge types by charge.
It calculates the in-group mean for each charge and variable, and visualizes the trends over time"""

import os

import numpy as np
import pandas as pd
import seaborn as sns


def find_global_stabilization_point(
    df,
    method="std",
    min_points=5,
    groups: list = ["Date", "Charge"],
    col="In_Group_Mean",
):
    """
    Identifies the global time point after which the system is most stable across all groups.

    Parameters:
        df: DataFrame with ['group1', 'group2', 'time', 'value']
        method: 'std' or 'range'
        min_points: Minimum number of time points required after cutoff per group
        groups: List of columns to group by
        col: Column to calculate instability on

    Returns:
        DataFrame with time and mean instability; also returns best time
    """
    all_times = sorted(df["time"].unique())
    results = []

    for t in all_times:
        group_stabilities = []

        for _, group_df in df.groupby(groups):
            subdf = group_df[group_df["time"] >= t].sort_values("time")
            values = subdf[col].values

            if len(values) < min_points:
                continue  # skip groups with too little data after t

            if method == "std":
                instability = np.std(values)
            elif method == "range":
                instability = np.max(values) - np.min(values)
            else:
                raise ValueError("method must be 'std' or 'range'")

            group_stabilities.append(instability)

        if len(group_stabilities) == 0:
            continue

        mean_instability = np.mean(group_stabilities)
        results.append({"time": t, "mean_instability": mean_instability})

    result_df = pd.DataFrame(results)
    best_row = result_df.loc[result_df["mean_instability"].idxmin()]
    return result_df, best_row


def find_osh_discharge_proportions(
    directory: str,
    file_name: str,
    save_name: str,
    save_path: str,
    save_plots: bool = False,
):
    """Finds and visualizes the proportions of different discharge types in the OSH dataset.

    Args:
        directory (str): The directory containing the data file.
        file_name (str): The name of the data file.
        save_name (str): The base name for saving output files.
        save_path (str): The directory to save the output files.
        save_plots (bool, optional): Whether to save the plots. Defaults to False.
    Raises:
        FileNotFoundError: If the data file is not found.
        ValueError: If the data file is empty or has an invalid format.

    """

    df = pd.read_csv(
        os.path.join(
            directory,
            file_name,
        )
    )

    # Calculate the in-group mean for each charge and variable, using the 'Total Discharged' as the total
    # Divide each variable in the same date by the total discharged for that date
    # Do not count up rows with 'Total Discharged' as the variable, but use to divide by charge and date

    # Get only discharge rows
    discharge_rows = [
        "Found Able",
        "Found Never Able",
        "Community Restoration",
        "Charges Dismissed or Released",
        "Discharged After Meeting 30-Day RL Notice Period",
        "Other",
        "Total Discharged",
    ]
    df_count = df[df["Variable"].isin(discharge_rows)]
    df_count_total = df_count[df_count["Variable"] != "Total Discharged"].fillna(0)
    df_count_values = df_count[df["Variable"] == "Total Discharged"]
    # Merge these two dataframes to get the total discharged for each date and charge
    df_count_total = df_count_total.merge(
        df_count_values[["Date", "Charge", "Value"]],
        on=["Date", "Charge"],
        suffixes=("", "_Total"),
    )
    # Get in group mean for each charge and variable
    df_count_total["In_Group_Mean"] = (
        df_count_total["Value"] / df_count_total["Value_Total"]
    )

    # Make a separate lineplot for each charge
    # Make each charge be on a different plot and have the 4 plots be in a grid
    df_count_total["Date"] = pd.to_datetime(df_count_total["Date"])
    df_count_total = df_count_total.sort_values(by=["Charge", "Date"])
    # Plot the in-group mean for each charge and variable over time
    sns.set_theme(style="whitegrid")
    g = sns.FacetGrid(
        df_count_total,
        col="Charge",
        hue="Variable",
        col_wrap=2,
    )
    g.map(sns.lineplot, "Date", "In_Group_Mean")
    # Put legend below the plots, not to the right
    g.set_axis_labels("Date", "In-Group Mean")
    g.add_legend(title="Variable", bbox_to_anchor=(0.3, -0.1), loc="upper center")
    # Make date labels more readable
    for ax in g.axes.flat:  # Iterate through each subplot
        for label in ax.get_xticklabels():
            label.set_rotation(45)  # Rotate x-axis labels for better readability
            label.set_ha("right")  # Set horizontal alignment to right

    # Save the plot if requested
    if save_plots:
        g.savefig(os.path.join(save_path, f"{save_name}_discharge_proportions.png"))

    # Detect stabilization in the time series
    df_count_total["time"] = pd.to_datetime(df_count_total["Date"]).astype(int) // 10**9
    result_df, best_point = find_global_stabilization_point(
        df_count_total,
        method="std",  # or "range"
        groups=["Charge", "Variable"],
        col="In_Group_Mean",
    )

    # Use our stabilization point to filter the data
    df_count_total = df_count_total[
        df_count_total["time"] > best_point["time"]
    ].reset_index(drop=True)
    df_count_total = df_count_total.drop(columns=["time"])
    # It appears that the trend stabilizes after 2024-05, so redo this plot from that date on
    df_count_total = df_count_total[df_count_total["Date"] >= "2024-05-01"]
    df_count_total = df_count_total.sort_values(by=["Charge", "Date"])
    # Plot the in-group mean for each charge and variable over time
    sns.set_theme(style="whitegrid")
    g = sns.FacetGrid(
        df_count_total,
        col="Charge",
        hue="Variable",
        col_wrap=2,
    )
    g.map(sns.lineplot, "Date", "In_Group_Mean")
    # Put legend below the plots, not to the right
    g.set_axis_labels("Date", "In-Group Mean")
    g.add_legend(title="Variable", bbox_to_anchor=(0.3, -0.1), loc="upper center")
    # Make date labels more readable
    for ax in g.axes.flat:  # Iterate through each subplot
        for label in ax.get_xticklabels():
            label.set_rotation(45)  # Rotate x-axis labels for better readability
            label.set_ha("right")  # Set horizontal alignment to right

    # Save the plot if requested
    if save_plots:
        g.savefig(
            os.path.join(save_path, f"{save_name}_discharge_proportions_stable.png")
        )

    # Now, get overall mean over time
    df_count_total_mean = (
        df_count_total.groupby(["Charge", "Variable"])["In_Group_Mean"]
        .mean()
        .round(3)
        .reset_index()
    )

    # Find out how much these add to, and add the difference from 1 to all categories by charge
    df_count_mean_overall = (
        df_count_total_mean.groupby("Charge")["In_Group_Mean"].sum().reset_index()
    )
    df_count_mean_overall["In_Group_Mean_needed"] = (
        1 - df_count_mean_overall["In_Group_Mean"]
    ) / len(discharge_rows)
    df_count_total_mean = df_count_total_mean.merge(
        df_count_mean_overall[["Charge", "In_Group_Mean_needed"]],
        on="Charge",
        how="left",
    )
    #
    df_count_total_mean["In_Group_Mean"] += df_count_total_mean["In_Group_Mean_needed"]

    df_count_total_mean.to_csv(
        os.path.join(save_path, f"{save_name}_discharge_proportions_mean.csv"),
        index=False,
    )


find_osh_discharge_proportions(
    os.path.join(
        os.getcwd(),
        "../OSH_Restoration_Limit_data/osh_a_a_restoration_limit_cohort2_through_2025-07.csv",
    )
)
