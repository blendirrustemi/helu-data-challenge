from __future__ import annotations

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def validate_gold_report(gold_df: pd.DataFrame) -> None:
    """
    Validate the final Gold report before saving.

    Checks the expected schema, required non-null fields, numeric metric columns,
    non-negative values, and month-start alignment for report_month.
    """
    required_columns = [
        "report_month",
        "platform",
        "subscription_type",
        "country",
        "acquisitions",
        "renewals",
        "cancellations",
        "mrr_eur",
    ]

    missing = [col for col in required_columns if col not in gold_df.columns]
    if missing:
        raise ValueError(f"Gold report is missing required columns: {missing}")

    if gold_df.empty:
        logger.warning("Gold report is empty – no rows to validate.")
        return

    numeric_cols = ["acquisitions", "renewals", "cancellations", "mrr_eur"]
    for col in numeric_cols:
        if not pd.api.types.is_numeric_dtype(gold_df[col]):
            raise ValueError(f"Column '{col}' in Gold report must be numeric.")

    if (gold_df[["acquisitions", "renewals", "cancellations"]] < 0).any().any():
        raise ValueError("Gold report contains negative event counts.")

    if (gold_df["mrr_eur"] < 0).any():
        raise ValueError("Gold report contains negative mrr_eur values.")

    if gold_df[required_columns].isna().any().any():
        raise ValueError("Gold report contains null values in required columns.")

    # report_month should be aligned to the first day of the month
    if not gold_df["report_month"].dt.is_month_start.all():
        raise ValueError(
            "Gold report contains report_month values that are not aligned to month start."
        )
