from __future__ import annotations

import logging

import country_converter as coco
import pandas as pd
from typing import Any

logger = logging.getLogger(__name__)


EVENT_TYPE_MAP = {
    "SUBSCRIPTION_STARTED": "acquisition",
    "SUBSCRIPTION_RENEWED": "renewal",
    "SUBSCRIPTION_CANCELLED": "cancellation",
    "NEW": "acquisition",
    "RENEW": "renewal",
    "CANCEL": "cancellation",
}

CURRENCY_MAP = {
    "EUR": "EUR",
    "EURO": "EUR",
    "USD": "USD",
    "GBP": "GBP",
}


def normalize_event_type(value: Any) -> str | None:
    if pd.isna(value):
        return None

    normalized = str(value).strip().upper()
    return EVENT_TYPE_MAP.get(normalized)


def normalize_country(series: pd.Series) -> pd.Series:
    """Normalize country names to ISO2 codes, with some basic cleaning."""
    cleaned = series.astype("string").str.strip()
    cleaned = cleaned.replace("", pd.NA)

    unique_values = cleaned.dropna().unique()

    country_map = {}
    for value in unique_values:
        country_map[value] = coco.convert(names=value, to="ISO2", not_found=None)

    mapped = cleaned.map(country_map)

    # Keep only 2-letter uppercase codes
    mapped = mapped.where(mapped.str.fullmatch(r"[A-Z]{2}"), pd.NA)

    # Drop known placeholder code
    mapped = mapped.replace("XX", pd.NA)

    return mapped


def normalize_currency(value: Any) -> str | None:
    if pd.isna(value):
        return None

    normalized = str(value).strip().upper()
    return CURRENCY_MAP.get(normalized)


def infer_renewal_period(row: pd.Series) -> str | None:
    current_value = row.get("renewal_period")
    if pd.notna(current_value):
        value = str(current_value).strip().lower()
        if value in {"monthly", "month"}:
            return "monthly"
        if value in {"yearly", "annual", "year"}:
            return "yearly"
        return value

    subscription_type = row.get("subscription_type")
    if pd.isna(subscription_type):
        return None

    subscription_type = str(subscription_type).strip().lower()

    if "monthly" in subscription_type:
        return "monthly"
    if "yearly" in subscription_type or "annual" in subscription_type:
        return "yearly"

    return None


def log_dropped_rows(before_count: int, after_count: int, reason: str) -> None:
    dropped = before_count - after_count
    if dropped > 0:
        logger.info("Dropped %s rows due to %s", dropped, reason)


def clean_silver_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate the Silver subscription events table.

    This step standardizes key fields, converts numeric values, removes rows missing
    required reporting fields, filters invalid event types, and deduplicates events
    by (platform, event_id).

    The goal is to keep only consistent records for downstream Gold reporting.
    """
    result = df.copy()

    result["amount"] = pd.to_numeric(result["amount"], errors="coerce")
    result["tax_amount"] = pd.to_numeric(result["tax_amount"], errors="coerce")

    result["event_type"] = result["event_type"].apply(normalize_event_type)
    result["country"] = normalize_country(result["country"])
    result["currency"] = result["currency"].apply(normalize_currency)
    result["renewal_period"] = result.apply(infer_renewal_period, axis=1)

    required_columns = [
        "platform",
        "event_id",
        "event_ts",
        "event_type",
        "customer_id",
        "country",
        "subscription_type",
        "currency",
        "amount",
    ]
    allowed_event_types = {"acquisition", "renewal", "cancellation"}

    before = len(result)
    result = result.dropna(subset=required_columns)
    log_dropped_rows(before, len(result), "missing required fields")

    before = len(result)
    result = result[result["event_type"].isin(allowed_event_types)]
    log_dropped_rows(before, len(result), "invalid event types")

    before = len(result)
    result = result.drop_duplicates(subset=["platform", "event_id"])
    log_dropped_rows(before, len(result), "duplicate records")

    return result.reset_index(drop=True)
