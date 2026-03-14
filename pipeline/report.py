from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


GOLD_GROUP_COLUMNS = [
    "report_month",
    "platform",
    "subscription_type",
    "country",
]


SUBSCRIPTION_KEY = [
    "platform",
    "customer_id",
    "subscription_type",
]


def add_report_month_column(
    df: pd.DataFrame,
    ts_col: str = "event_ts",
) -> pd.DataFrame:
    """
    Derive a month bucket from UTC-normalized event timestamps.

    For this exercise, reporting months are derived from normalized UTC timestamps.
    but in production, month bucketing may need to follow the business timezone.
    """
    result = df.copy()
    result["report_month"] = (
        result[ts_col].dt.tz_localize(None).dt.to_period("M").dt.to_timestamp()
    )
    return result


def normalize_monthly_amount(row: pd.Series) -> float:
    """
    Convert a subscription amount into monthly recurring value.

    Monthly plans keep their amount as it is, while yearly plans are divided by 12.
    Unsupported or missing renewal periods return 0.0.
    """
    amount = row["amount"]
    renewal_period = row["renewal_period"]

    if pd.isna(amount) or pd.isna(renewal_period):
        return 0.0

    renewal_period = str(renewal_period).strip().lower()

    if renewal_period == "monthly":
        return float(amount)
    if renewal_period == "yearly":
        return float(amount) / 12

    return 0.0


def prepare_exchange_rates(exchange_rates_df: pd.DataFrame) -> pd.DataFrame:
    result = exchange_rates_df.copy()

    result["date"] = pd.to_datetime(result["date"], errors="coerce")
    result["currency"] = result["currency"].astype(str).str.strip().str.upper()
    result["report_month"] = result["date"].dt.to_period("M").dt.to_timestamp()
    result["rate_to_eur"] = pd.to_numeric(result["rate_to_eur"], errors="coerce")

    return result[["report_month", "currency", "rate_to_eur"]]


def _filter_events_up_to_month(
    result: pd.DataFrame, report_month: pd.Timestamp
) -> pd.DataFrame:
    next_month_start = pd.Timestamp(report_month) + pd.offsets.MonthBegin(1)
    return result[result["event_ts"].dt.tz_localize(None) < next_month_start].copy()


def _latest_events_per_subscription(month_events: pd.DataFrame) -> pd.DataFrame:
    return month_events.sort_values("event_ts").drop_duplicates(
        subset=SUBSCRIPTION_KEY, keep="last"
    )


def _active_subscriptions(latest_events: pd.DataFrame) -> pd.DataFrame:
    active = latest_events[latest_events["event_type"] != "cancellation"].copy()
    active["monthly_amount"] = active.apply(
        normalize_monthly_amount,
        axis=1,
    )
    active = active[active["monthly_amount"] > 0].copy()
    active["currency"] = active["currency"].astype(str).str.strip().str.upper()
    return active


def _apply_fx_and_aggregate_mrr(
    active_subscriptions: pd.DataFrame,
    exchange_rates: pd.DataFrame,
    report_month: pd.Timestamp,
) -> tuple[pd.DataFrame, int]:
    active_subscriptions = active_subscriptions.copy()
    active_subscriptions["report_month"] = report_month

    active_subscriptions = active_subscriptions.merge(
        exchange_rates,
        on=["report_month", "currency"],
        how="left",
    )

    active_subscriptions.loc[
        active_subscriptions["currency"] == "EUR", "rate_to_eur"
    ] = 1.0

    missing_fx_count = active_subscriptions["rate_to_eur"].isna().sum()

    active_subscriptions = active_subscriptions.dropna(subset=["rate_to_eur"]).copy()

    active_subscriptions["mrr_eur"] = (
        active_subscriptions["monthly_amount"] * active_subscriptions["rate_to_eur"]
    )

    monthly_mrr = active_subscriptions.groupby(GOLD_GROUP_COLUMNS, as_index=False)[
        "mrr_eur"
    ].sum()

    return monthly_mrr, int(missing_fx_count)


def build_mrr_report(
    silver_df: pd.DataFrame,
    exchange_rates_df: pd.DataFrame,
) -> pd.DataFrame:
    result = add_report_month_column(silver_df)
    exchange_rates = prepare_exchange_rates(exchange_rates_df)

    report_months = sorted(result["report_month"].dropna().unique())
    monthly_results: list[pd.DataFrame] = []
    total_missing_fx_rows = 0

    for report_month in report_months:
        month_events = _filter_events_up_to_month(result, report_month)
        latest_events = _latest_events_per_subscription(month_events)
        active = _active_subscriptions(latest_events)
        monthly_mrr, missing_fx = _apply_fx_and_aggregate_mrr(
            active,
            exchange_rates,
            report_month,
        )
        monthly_results.append(monthly_mrr)
        total_missing_fx_rows += missing_fx

    if total_missing_fx_rows > 0:
        logger.warning(
            "Excluded %s active subscription rows from MRR due to missing FX rates",
            total_missing_fx_rows,
        )

    if not monthly_results:
        return pd.DataFrame(columns=GOLD_GROUP_COLUMNS + ["mrr_eur"])

    final_mrr = pd.concat(monthly_results, ignore_index=True)
    final_mrr["mrr_eur"] = final_mrr["mrr_eur"].round(2)

    return final_mrr


def build_gold_report(
    silver_df: pd.DataFrame, exchange_rates_df: pd.DataFrame
) -> pd.DataFrame:
    event_counts = build_event_counts(silver_df)
    mrr_report = build_mrr_report(silver_df, exchange_rates_df)

    gold = event_counts.merge(
        mrr_report,
        on=["report_month", "platform", "subscription_type", "country"],
        how="outer",
    )

    for col in ["acquisitions", "renewals", "cancellations"]:
        gold[col] = gold[col].fillna(0).astype(int)

    gold["mrr_eur"] = gold["mrr_eur"].fillna(0.0).round(2)

    return gold.sort_values(
        ["report_month", "platform", "subscription_type", "country"]
    ).reset_index(drop=True)


def build_event_counts(silver_df: pd.DataFrame) -> pd.DataFrame:
    result = add_report_month_column(silver_df)

    grouped = (
        result.groupby(GOLD_GROUP_COLUMNS + ["event_type"])
        .size()
        .reset_index(name="event_count")
    )

    pivoted = grouped.pivot_table(
        index=GOLD_GROUP_COLUMNS,
        columns="event_type",
        values="event_count",
        fill_value=0,
    ).reset_index()

    pivoted.columns.name = None

    pivoted = pivoted.rename(
        columns={
            "acquisition": "acquisitions",
            "renewal": "renewals",
            "cancellation": "cancellations",
        }
    )

    for col in ["acquisitions", "renewals", "cancellations"]:
        if col not in pivoted.columns:
            pivoted[col] = 0

    pivoted["acquisitions"] = pivoted["acquisitions"].astype(int)
    pivoted["renewals"] = pivoted["renewals"].astype(int)
    pivoted["cancellations"] = pivoted["cancellations"].astype(int)

    return pivoted[
        [
            "report_month",
            "platform",
            "subscription_type",
            "country",
            "acquisitions",
            "renewals",
            "cancellations",
        ]
    ]
