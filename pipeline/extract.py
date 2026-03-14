from __future__ import annotations

from io import StringIO

import pandas as pd
import requests


def fetch_apfel_subscriptions(base_url: str) -> pd.DataFrame:
    response = requests.get(f"{base_url}/apfel/subscriptions", timeout=30)
    response.raise_for_status()

    payload = response.json()
    return pd.DataFrame(payload["events"])


def fetch_fenster_subscriptions(base_url: str) -> pd.DataFrame:
    response = requests.get(f"{base_url}/fenster/subscriptions", timeout=30)
    response.raise_for_status()

    return pd.read_csv(StringIO(response.text))


def fetch_exchange_rates(base_url: str) -> pd.DataFrame:
    response = requests.get(f"{base_url}/exchange-rates", timeout=30)
    response.raise_for_status()

    return pd.read_csv(StringIO(response.text))
