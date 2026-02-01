import requests
import pandas as pd
from typing import List
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config.settings import settings
from datetime import timedelta, date

def fetch_data_pse_inner(bdate: date, select_columns: List[str] = None) -> pd.DataFrame:

    params = {"$filter": f"business_date eq '{bdate.isoformat()}'",}

    if select_columns is not None:
        params["$select"] = ",".join(select_columns)

    session = requests.Session()
    retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
            )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    try:
        response = session.get(settings.API_URL_PSE, params=params, timeout=20)
        response.raise_for_status()
        raw_json = response.json()
        if "value" in raw_json:
            df = pd.DataFrame(raw_json["value"])
            return df
        else:
            return pd.DataFrame()
    except requests.exceptions.RequestException:
        return pd.DataFrame()

def fetch_data_pse(start_date: date, end_date: date, select_columns: List[str]) -> pd.DataFrame:

    current_date = start_date
    df = pd.DataFrame()

    while current_date <= end_date:
        day_df = fetch_data_pse_inner(current_date, select_columns)
        df = pd.concat([df, day_df], ignore_index=True)
        current_date += timedelta(days=1)

    return df

def fetch_data_meteo(
        latitude: float,
        longitude: float,
        select_columns: List[str],
        start_date: date = None,
        end_date: date = None
) -> pd.DataFrame:
    params = {"timezone": "Europe/Berlin", "latitude": latitude, "longitude": longitude, "hourly": select_columns}
    if start_date is not None and end_date is not None:
        api_url = settings.API_URL_METEO_HIST
        params["start_date"] = start_date.isoformat()
        params["end_date"] = end_date.isoformat()
    else:
        api_url = settings.API_URL_METEO_FRCST

    session = requests.Session()
    retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
            )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        response = session.get(api_url, params=params, timeout=20)
        response.raise_for_status()
        raw_json = response.json()
        if "hourly" in raw_json:
            df = pd.DataFrame(raw_json["hourly"])
            return df
        else:
            return pd.DataFrame()
    except requests.exceptions.RequestsException:
        return pd.DataFrame()

