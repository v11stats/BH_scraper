import os
import re
from datetime import date
from time import sleep

import pandas as pd
import requests


def update_aid_and_assist_data(directory):
    """Query the Oregon Health Authority for the latest Aid & Assist data.

    Add to the folder, if there are updates.

    Note: Errors often signify there is no new data.

    Args:
        directory (str): The directory to save the downloaded reports and check for existing reports.
    Raises:
        requests.RequestException: If there is an error downloading the report.
    """

    # Check if there is a report for months we do not have, up to the current month.
    # If so, download it.

    # Go through our directory and find the latest date in the format YYYY-MM or YYYY.MM
    date_match = None
    for file_ in os.listdir(directory):
        if file_.endswith(".pdf"):
            match = re.search(r"\d{4}[-\.]\d{2}", file_)
            if match:
                date_str = match.group()
                date_temp = pd.to_datetime(date_str)
                if not date_match or date_temp > date_match:
                    date_match = date_temp

    current_date = date.today()
    while (
        date_match is None
        or date_match.month < current_date.month
        or date_match.year < current_date.year
    ):
        # If we don't have the latest month, update the date_match to the current month
        date_match = date_match + pd.DateOffset(months=1)

        url = f"https://www.oregon.gov/oha/OSH/reports/{date_match.strftime('%Y-%m')}-OSH-Forensic-Admission-Discharge-Dashboard.pdf"

        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad responses
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


def update_census_data(directory):
    """Query the Oregon Health Authority for the latest census data.

    Add to the folder, if there are updates.

    Note: Errors often signify there is no new data.

    Args:
        directory (str): The directory to save the downloaded reports and check for existing reports.
    Raises:
        requests.RequestException: If there is an error downloading the report.
    """

    # Check if there is a report for months we do not have, up to the current month.
    # If so, download it.

    # Go through our directory and find the latest date in the format YYYY-MM or YYYY.MM
    date_match = None
    for file_ in os.listdir(directory):
        if file_.endswith(".pdf"):
            match = re.search(r"\d{4}[-\.]\d{2}-\d{2}", file_)
            if match:
                date_str = match.group()
                date_temp = pd.to_datetime(date_str)
                if not date_match or date_temp > date_match:
                    date_match = date_temp

    current_date = date.today()
    while date_match != current_date:
        # If we don't have the latest data, update the date_match to the current date
        date_match = date_match + pd.DateOffset(days=1)

        url = f"https://www.oregon.gov/oha/OSH/reports/Aid-and-assist-census-by-county-{date_match.strftime('%Y-%m-%d')}.pdf"

        try:
            sleep(1)
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad responses
            with open(
                os.path.join(
                    directory,
                    f"Aid-and-assist-census-by-county-{date_match.strftime('%Y-%m-%d')}.pdf",
                ),
                "wb",
            ) as f:
                f.write(response.content)
            print("Successfully downloaded:", url)
        except requests.RequestException as e:
            print(f"{e}")
