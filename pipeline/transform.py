from __future__ import annotations

import pandas as pd

from pipeline.schemas import (
    APFEL_COLUMN_MAP,
    FENSTER_COLUMN_MAP,
    SILVER_COLUMNS,
)


def ensure_silver_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the dataframe contains all Silver columns.
    Missing columns are added with None values.
    """
    result = df.copy()

    for col in SILVER_COLUMNS:
        if col not in result.columns:
            result[col] = None

    return result[SILVER_COLUMNS]


def transform_to_silver(
    df: pd.DataFrame,
    column_map: dict[str, str],
    platform: str,
) -> pd.DataFrame:
    """
    Rename source columns into the Silver schema and attach platform type.
    """
    result = df.rename(columns=column_map).copy()
    result["platform"] = platform

    return ensure_silver_columns(result)


def transform_apfel_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Apfel raw events into Silver shape and parse Apfel timestamps.
    """
    result = transform_to_silver(df, APFEL_COLUMN_MAP, platform="apfel")

    result["event_ts"] = pd.to_datetime(result["event_ts"], errors="coerce", utc=True)
    result["customer_created_at"] = pd.to_datetime(
        result["customer_created_at"], errors="coerce", utc=True
    )

    return result


def transform_fenster_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Fenster raw events into Silver shape and parse Fenster timestamps.
    """
    result = transform_to_silver(df, FENSTER_COLUMN_MAP, platform="fenster")

    result["event_ts"] = pd.to_datetime(
        result["event_ts"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
        utc=True,
    )
    result["customer_created_at"] = pd.to_datetime(
        result["customer_created_at"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
        utc=True,
    )

    return result


def combine_silver_events(
    apfel_df: pd.DataFrame,
    fenster_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine both Silver-shaped datasets into one unified events dataframe.
    """
    return pd.concat([apfel_df, fenster_df], ignore_index=True)
