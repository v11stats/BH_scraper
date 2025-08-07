"""This script investigates the OSH restoration limit data, focusing on discharge types by charge.
It calculates the in-group mean for each charge and variable, and visualizes the trends over time"""

import os

import pandas as pd
import seaborn as sns

df = pd.read_csv(
    os.path.join(
        os.getcwd(),
        "../OSH_Restoration_Limit_data/osh_a_a_restoration_limit_cohort2_through_2025-07.csv",
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


# Now, get overall mean over time
df_count_total_mean = (
    df_count_total.groupby(["Charge", "Variable"])["In_Group_Mean"]
    .mean()
    .round(3)
    .reset_index()
)

# Verify these means basically add to 1 for each charge
df_count_total_mean.groupby("Charge")["In_Group_Mean"].sum().reset_index()
