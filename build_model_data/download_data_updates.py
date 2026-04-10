import os
import re
from datetime import date
from time import sleep

import pandas as pd
import requests


def update_aid_and_assist_data(directory, starting_date=None):
    """Query the Oregon Health Authority for the latest Aid & Assist data.

    Add to the folder, if there are updates.

    Note: Errors often signify there is no new data.

    Args:
        directory (str): The directory to save the downloaded reports and check for existing reports.
        starting_date (str, optional): The starting date in 'YYYY-MM' or 'YYYY.MM' format to begin checking for updates. Defaults to None.
    Raises:
        requests.RequestException: If there is an error downloading the report.
        ValueError: If no valid starting date is found.
    """

    used_starting_date = False
    if starting_date:
        date_match = pd.to_datetime(starting_date)
        used_starting_date = True
    else:
        date_match = None
        for file_ in os.listdir(directory):
            if file_.endswith(".pdf"):
                match = re.search(r"\d{4}[-\.]\d{2}", file_)
                if match:
                    date_str = match.group()
                    date_temp = pd.to_datetime(date_str)
                    if not date_match or date_temp > date_match:
                        date_match = date_temp
        if date_match is None:
            raise ValueError(
                "No existing files found to determine starting date. Provide a starting_date."
            )

    current_date = date.today()
    while date_match.month < current_date.month or date_match.year < current_date.year:
        if not used_starting_date:
            date_match = date_match + pd.DateOffset(months=1)
        else:
            used_starting_date = False

        url = f"https://www.oregon.gov/oha/OSH/reports/{date_match.strftime('%Y-%m')}-OSH-Forensic-Admission-Discharge-Dashboard.pdf"

        try:
            sleep(1)
            response = requests.get(url)
            response.raise_for_status()
            with open(
                os.path.join(
                    directory,
                    f"{date_match.strftime('%Y-%m')}-OSH-Forensic-Admission-Discharge-Dashboard.pdf",
                ),
                "wb",
            ) as f:
                f.write(response.content)
            print("Successfully downloaded:", url)
        except requests.RequestException as e:
            print(f"{e}")
            continue


def update_census_data(directory, starting_date=None):
    """Query the Oregon Health Authority for the latest Aid & Assist census by county data.

    Uses the OHA SharePoint REST API to fetch the list of census report filenames,
    then downloads any that are not already present in the directory.

    Args:
        directory (str): The directory to save the downloaded reports and check for existing reports.
        starting_date (str, optional): Only download reports on or after this date ('YYYY-MM-DD'). Defaults to None (downloads all missing files).
    Raises:
        requests.RequestException: If the SharePoint API call fails.
    """

    # Fetch the list of census report filenames from the SharePoint REST API
    api_url = (
        "https://www.oregon.gov/oha/OSH/_api/web/GetList('/oha/OSH/reports')/items"
        "?$select=FileLeafRef&$filter=Report_x0020_type eq 'census'&$top=500"
    )
    headers = {"Accept": "application/json;odata=verbose"}

    response = requests.get(api_url, headers=headers)
    response.raise_for_status()

    results = response.json()["d"]["results"]
    remote_files = {item["FileLeafRef"] for item in results}

    existing_files = set(os.listdir(directory))

    cutoff = pd.to_datetime(starting_date) if starting_date else None

    for filename in sorted(remote_files):
        if filename in existing_files:
            continue

        if cutoff is not None:
            match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
            if match and pd.to_datetime(match.group()) < cutoff:
                continue

        url = f"https://www.oregon.gov/oha/OSH/reports/{filename}"
        try:
            sleep(1)
            r = requests.get(url)
            r.raise_for_status()
            with open(os.path.join(directory, filename), "wb") as f:
                f.write(r.content)
            print("Successfully downloaded:", url)
        except requests.RequestException as e:
            print(f"Failed to download {filename}: {e}")


def update_docket_data(directory):
    """Query the Oregon Court data each M, T, W.

    Add to the folder, if there are updates.

    Args:
        directory (str): The directory to save the downloaded reports and check for existing reports.
    Raises:
        requests.RequestException: If there is an error downloading the report.
    """

    date_match = None
    for file_ in os.listdir(directory):
        if file_.endswith(".pdf"):
            match = re.search(r"\d{4}[-\.]\d{2}-\d{2}", file_)
            if match:
                date_str = match.group()
                date_temp = pd.to_datetime(date_str)
                if not date_match or date_temp > date_match:
                    date_match = date_temp
    if date_match is None:
        date_match = pd.Timestamp(date.today()) - pd.DateOffset(days=1)

    current_date = pd.Timestamp(date.today())
    if date_match < current_date:
        url = "https://www.mcda.us/Court_Appearance_List.pdf"

        try:
            sleep(1)
            response = requests.get(url, verify=False)
            response.raise_for_status()
            with open(
                os.path.join(
                    directory,
                    f"{current_date.strftime('%Y-%m-%d')}-Court_Appearance_List.pdf",
                ),
                "wb",
            ) as f:
                f.write(response.content)
            print("Successfully downloaded:", url)
        except requests.RequestException as e:
            print(f"{e}")
