import os
import re
import subprocess

import pandas as pd
import tabula
from pypdf import PdfReader
from tqdm import tqdm

from download_data_updates import update_aid_and_assist_data, update_census_data

pd.set_option("future.no_silent_downcasting", True)


def find_java():
    """Find the Java installation path.

    Returns:
        str: The path to the Java installation, or None if not found.
    """
    result = subprocess.run(
        ["where", "java"], capture_output=True, text=True, shell=True
    )
    print("Java locations:", result.stdout)
    return result.stdout


def setup_java_environment():
    """Set up JAVA_HOME environment variable for tabula

    Returns:
        str: The JAVA_HOME directory if found, else None.
    """
    import glob

    # Find Java executable first
    find_java()

    try:
        # Try to get Java version and home directory
        java_home_result = subprocess.run(
            ["java", "-XshowSettings:properties", "-version"],
            capture_output=True,
            text=True,
            shell=True,
        )

        # Parse the output to find java.home
        for line in java_home_result.stderr.split("\n"):
            if "java.home" in line:
                java_home = line.split("=")[1].strip()
                os.environ["JAVA_HOME"] = java_home
                print(f"Set JAVA_HOME to: {java_home}")
                return java_home

        # Fallback: try common Oracle Java paths
        possible_paths = [
            r"C:\Program Files\Java\jre1.8.0_*",
            r"C:\Program Files\Java\jdk1.8.0_*",
            r"C:\Program Files\Java\jdk-*",
            r"C:\Program Files (x86)\Java\jre1.8.0_*",
        ]

        for pattern in possible_paths:
            matches = glob.glob(pattern)
            if matches:
                java_home = matches[0]  # Take the first match
                os.environ["JAVA_HOME"] = java_home
                print(f"Set JAVA_HOME to: {java_home}")
                return java_home

        print("Could not automatically determine JAVA_HOME")
        return None

    except Exception as e:
        print(f"Error setting up Java: {e}")
        return None


def process_aa_admit_discharge_timeseries(directory):
    """Process Aid & Assist timeseries data from PDF files.

    Args:
        directory (str): The directory containing the PDF files.

    Raises:
        ValueError: If the directory does not contain valid PDF files.
    """

    # Set up Java environment before using tabula
    setup_java_environment()
    # Check for new data
    update_aid_and_assist_data(directory)

    # set up 4 dataframes to append data from each file
    admission_list_df = pd.DataFrame()
    patients_admitted_df = pd.DataFrame()
    no_longer_needing_hloc_df = pd.DataFrame()
    patients_discharged_df = pd.DataFrame()
    # Iterate through all PDF files in the directory
    for file_ in tqdm(os.listdir(directory)):
        if file_.endswith(".pdf"):
            # Extract full date in format YYYY-MM or YYYY.MM
            date_match = re.search(r"\d{4}[-\.]\d{2}", file_)
            date_match = date_match.group() if date_match else None
            if date_match is None:
                raise ValueError(f"Date not found in filename {file_}.")
            date_match = pd.to_datetime(date_match)

            # Identify the page with the aid and assist data
            try:
                reader = PdfReader(os.path.join(directory, file_))
                j = 1
                for page in reader.pages:
                    text = page.extract_text()
                    if re.search("\\s+Aid & Assist\\s+", text):
                        if re.search("Baker", text):
                            if re.search("Low High Count", text):
                                break
                    j += 1
            except Exception as e:
                print(f"Error extracting text with pypdf: {e}")
            if j == len(reader.pages) + 1:
                print(f"Aid & Assist page not found in {file_}. Skipping.")
                continue
            tables = tabula.read_pdf(
                os.path.join(directory, file_),
                pages=j,
                multiple_tables=False,
                stream=True,
                lattice=True,
            )
            assert len(tables) == 1, (
                f"Expected 1 table, found {len(tables)} in {file_} on page {j}"
            )
            df = tables[0]

            # This data has a specific format. There are 4 main groups of columns:
            # 1. Admission List (4 columns with 2 rows of headers)
            # 2. Patients Admitted (4 columns with 2 rows of headers)
            # 3. No longer needing HLOC (8 columns with 3 rows of headers)
            # 4. Patients Discharged for Community Restoration (8 columns with 3 rows of headers)
            # We will need to process these groups separately to create a tidy dataframe.
            # Each one of these groups will be its own timeseries.
            # The first column is also the county information, with its title in the 2nd level.

            # Check if the second row contains 'Admission List'
            assert any("Avg Days" in str(s) for s in df.iloc[1, :].tolist()), (
                "This is not the correct column"
            )
            df.columns = df.iloc[1, :].to_list()  # Use the second row as column names
            if not pd.notna(df.columns.tolist()[0]):
                df = df.rename(columns={df.columns.tolist()[0]: "County"})
            df = df.rename(columns={df.columns.tolist()[-1]: "Avg Days"})
            # Drop first two rows of dataframe
            df = df.drop(index=[0, 1]).reset_index(drop=True)
            # the dataframe's columns need to be renamed when they're numbers to be LOCUS_<number>
            df.columns = [
                f"LOCUS_{int(i)}" if (isinstance(i, float)) and (not pd.isna(i)) else i
                for i in df.columns
            ]
            df["date"] = date_match
            counties = df.iloc[:, 0]
            # two of these counties are spelled incorrectly, so we will fix them
            counties = counties.replace({"Gil liam": "Gilliamy", "Yamhil l": "Yamhill"})
            # why is this not working???
            for i in list(range(df.shape[1])):
                if (
                    df.columns.tolist()[i] != "County"
                    and df.columns.tolist()[i] != "date"
                ):
                    df.iloc[:, i] = pd.to_numeric(df.iloc[:, i], errors="coerce")

            # Important checks.
            # 1) Count/Sum columns:
            # only include columns 2, 6, 10-17, 18-24
            check_columns = [2, 6] + list(range(10, 17)) + list(range(18, 25))
            count_columns = [i - 1 for i in check_columns]  # Adjust for zero-indexing
            # Check if the last row is the sum of the previous for these columns
            assert (
                df.iloc[-1, count_columns]
                .fillna(0)
                .infer_objects(copy=False)
                .astype(int)
                == df.iloc[:-1, count_columns].sum(axis=0, skipna=True)
            ).all(), "Total row does not match sum of previous rows"

            # 2) Avg columns:
            # only include columns 3, 7, 17, 25
            # The following shows that the original data has issues... so skip this check.
            # avg_columns = [3, 7, 17, 25]
            # avg_columns = [i - 1 for i in avg_columns]  # Adjust for zero-indexing
            # Check if the last row is the average of the previous for these columns
            # Since the average is rounded to 1 decimal place, we will round the mean to 1 decimal place
            # and then compare.
            # assert (
            #     df.iloc[-1, avg_columns]
            #     == df.iloc[:-1, avg_columns].mean(axis=0, skipna=True).round(decimals=1)
            # ).all(), "Average row does not match average of previous rows"

            # 3) Low:
            # Only include columns 4, 8
            # Take the minimum of the previous rows for these columns.
            low_columns = [4, 8]
            low_columns = [i - 1 for i in low_columns]  # Adjust for zero-indexing
            assert (
                df.iloc[-1, low_columns].fillna(0).infer_objects(copy=False).astype(int)
                == df.iloc[:-1, low_columns].min(axis=0, skipna=True)
            ).all(), "Low row does not match minimum of previous rows"

            # 4) High:
            # Only include columns 5, 9
            high_columns = [5, 9]
            high_columns = [i - 1 for i in high_columns]  # Adjust for zero-indexing
            # Check if the last row is the maximum of the previous for these columns
            assert (
                df.iloc[-1, high_columns]
                .fillna(0)
                .infer_objects(copy=False)
                .astype(int)
                == df.iloc[:-1, high_columns].max(axis=0, skipna=True)
            ).all(), "High row does not match maximum of previous rows"

            # Build a new dataset with each of these groups as separate dataframes
            admission_list_df_temp = df.iloc[:, 1:5]
            admission_list_df_temp.insert(0, "County", counties)
            admission_list_df_temp.insert(1, "Date", df["date"])
            # use the next 4 but also add the county column
            patients_admitted_df_temp = df.iloc[:, 5:9]
            patients_admitted_df_temp.insert(0, "County", counties)
            patients_admitted_df_temp.insert(1, "Date", df["date"])

            no_longer_needing_hloc_df_temp = df.iloc[:, 9:17]
            no_longer_needing_hloc_df_temp.insert(0, "County", counties)
            no_longer_needing_hloc_df_temp.insert(1, "Date", df["date"])

            # use the next 8 but also add the county column
            patients_discharged_df_temp = df.iloc[:, 17:25]
            patients_discharged_df_temp.insert(0, "County", counties)
            patients_discharged_df_temp.insert(1, "Date", df["date"])

            # turn these dataframes into long format, with county and date as identifiers
            admission_list_df_temp = pd.melt(
                admission_list_df_temp,
                id_vars=["County", "Date"],
                var_name="Variable",
                value_name="Value",
            )
            patients_admitted_df_temp = pd.melt(
                patients_admitted_df_temp,
                id_vars=["County", "Date"],
                var_name="Variable",
                value_name="Value",
            )
            no_longer_needing_hloc_df_temp = pd.melt(
                no_longer_needing_hloc_df_temp,
                id_vars=["County", "Date"],
                var_name="Variable",
                value_name="Value",
            )
            patients_discharged_df_temp = pd.melt(
                patients_discharged_df_temp,
                id_vars=["County", "Date"],
                var_name="Variable",
                value_name="Value",
            )

            # append the temporary dataframes to the main dataframes
            admission_list_df = pd.concat(
                [admission_list_df, admission_list_df_temp], ignore_index=True
            )
            patients_admitted_df = pd.concat(
                [patients_admitted_df, patients_admitted_df_temp], ignore_index=True
            )
            no_longer_needing_hloc_df = pd.concat(
                [no_longer_needing_hloc_df, no_longer_needing_hloc_df_temp],
                ignore_index=True,
            )
            patients_discharged_df = pd.concat(
                [patients_discharged_df, patients_discharged_df_temp],
                ignore_index=True,
            )

    # Save each dataframe to a CSV file
    admission_list_df.to_csv(
        os.path.join(
            directory,
            f"osh_a_a_admission_list_through_{max(admission_list_df['Date']).strftime('%Y-%m')}.csv",
        ),
        index=False,
    )
    patients_admitted_df.to_csv(
        os.path.join(
            directory,
            f"osh_a_a_patients_admitted_through_{max(patients_admitted_df['Date']).strftime('%Y-%m')}.csv",
        ),
        index=False,
    )
    no_longer_needing_hloc_df.to_csv(
        os.path.join(
            directory,
            f"osh_a_a_no_longer_needing_hloc_through_{max(no_longer_needing_hloc_df['Date']).strftime('%Y-%m')}.csv",
        ),
        index=False,
    )
    patients_discharged_df.to_csv(
        os.path.join(
            directory,
            f"osh_a_a_patients_discharged_through_{max(patients_discharged_df['Date']).strftime('%Y-%m')}.csv",
        ),
        index=False,
    )


def fix_incorrect_census_and_a_a_cols(df: pd.DataFrame):
    """Fix incorrect column names in the A&A census data.

    Args:
        df (pd.DataFrame): The dataframe to fix.

    Returns:
        pd.DataFrame: The fixed dataframe.
    """
    # Fix the column names
    df.columns = [
        col.replace(".315 A&A Census Census", "A&A Census").replace(
            "nan", ".315 Census"
        )
        for col in df.columns
    ]
    # This error also results in an incorrect total.
    df.loc[df["County"] == "Total", "A&A Census"] = 379
    df.loc[df["County"] == "Total", ".315 Census"] = 0
    return df


def process_a_a_census_timeseries(directory: str):
    """Process the A&A census timeseries data.

    Args:
        directory (str): The directory containing the PDF files.

    Raises:
        ValueError: If the file being processed does not contain a valid date in the filename.
        AssertionError: If the expected number of tables is not found in the PDF file.
        AssertionError: If the total row does not match the sum of previous rows for numeric columns
    """
    update_census_data(directory)
    # Set up Java environment before using tabula
    setup_java_environment()

    census_df = pd.DataFrame()
    # Iterate through all PDF files in the directory
    for file_ in tqdm(os.listdir(directory)):
        if file_.endswith(".pdf"):
            # Extract full date in format YYYY-MM-DD
            date_match = re.search(r"\d{4}-\d{2}-\d{2}", file_)
            date_match = date_match.group() if date_match else None
            if date_match is None:
                raise ValueError(f"Date not found in filename {file_}.")
            date_match = pd.to_datetime(date_match)

            tables = tabula.read_pdf(
                os.path.join(directory, file_),
                pages="all",
                multiple_tables=False,
                stream=True,
                lattice=False,
            )
            assert len(tables) == 1, (
                f"Expected 1 table, found {len(tables)} in {file_} on page 1"
            )
            df = tables[0]
            # The paragraph break is splitting the column names into the col and first row.
            # Combine them into a column name and drop the first row.
            df.columns = ["" if "Unnamed" in str(col) else col for col in df.columns]
            df.columns = [
                f"{col} {str(df.iloc[0, i])}".strip()
                for i, col in enumerate(df.columns)
            ]
            df.columns = [col.replace(".1", "") for col in df.columns]
            # If one of the columns is only "from Prev. Week", make it "Change from Prev. Week"
            df.columns = [
                col if col != "from Prev. Week" else "Change from Prev. Week"
                for col in df.columns
            ]
            df.columns = [
                col if col != "State Pop." else "% of State Pop." for col in df.columns
            ]
            df.columns = [
                col if col != "vs. Pop. Dif." else "Census vs. Pop. Dif."
                for col in df.columns
            ]
            # If this is our one problem file, fix it
            if file_ == "Aid-and-assist-census-by-county-2024-08-19.pdf":
                df = fix_incorrect_census_and_a_a_cols(df)
            # We have specific columns, so make sure they're all there
            assert set(df.columns).issuperset(
                {
                    "County",
                    ".370 Census",
                    ".365 Census",
                    ".315 Census",
                    "A&A Census",
                    "Change from Prev. Week",
                    "% of Census",
                    "% of State Pop.",
                    "Census vs. Pop. Dif.",
                    "Fel.",
                    "% Fel.",
                    "Misd.",
                    "% Misd.",
                    "None Listed",
                }
            ), f"Missing expected columns in {file_}"
            df = df.drop(index=0).reset_index(drop=True)
            # Remove % and make the data numeric
            df = df.replace("%", "", regex=True)
            df = df.replace("#DIV/0!", pd.NA, regex=True)
            df = df.replace({r"[‐‑–—−]": "-"}, regex=True)
            counties = df["County"]
            # Turn all columns to numeric, except the first
            df = df.iloc[:, 1:].apply(pd.to_numeric)
            df.insert(0, "County", counties)
            df["Date"] = date_match

            # Each column should sum to the bottom row, so we will check that
            # % Fel and % Misd are averages, so we won't include those.
            # Do not include 10 or 12, or the first or last column.
            sum_columns = df.columns[1:-1].difference(["% Fel.", "% Misd."])
            # Turn this list into numeric indices
            sum_columns = [df.columns.get_loc(col) for col in sum_columns]
            if not (
                df.iloc[-1, sum_columns].fillna(0)
                == df.iloc[:-1, sum_columns].sum(axis=0, skipna=True).round(0)
            ).all():
                # It may be a rounding issue so we will allow for a difference of 1
                assert (
                    abs(
                        df.iloc[-1, sum_columns]
                        - df.iloc[:-1, sum_columns].sum(axis=0, skipna=True).round(0)
                    )
                    <= 1
                ).all(), f"Total row does not match sum of previous rows in {file_}"

            # turn this df into long format
            df = pd.melt(
                df,
                id_vars=["County", "Date"],
                var_name="Variable",
                value_name="Value",
            )

            census_df = pd.concat([census_df, df], ignore_index=True)

    # Save the combined census dataframe to a CSV file
    census_df.to_csv(
        os.path.join(
            directory,
            f"osh_a_a_census_timeseries_through_{max(census_df['Date']).strftime('%Y-%m')}.csv",
        ),
        index=False,
    )


process_aa_admit_discharge_timeseries(
    directory=os.path.join(os.getcwd(), "../OSH AandA Admit Discharge")
)
process_a_a_census_timeseries(
    directory=os.path.join(os.getcwd(), "../OSH AandA Census")
)
