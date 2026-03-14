import pandas as pd
import pytest

from pipeline.report import build_mrr_report
from pipeline.validation import validate_gold_report


def test_build_mrr_report_excludes_cancelled_subscriptions() -> None:
    # One subscription that is acquired, renewed, then cancelled
    silver = pd.DataFrame(
        [
            {
                "platform": "apfel",
                "customer_id": "c1",
                "subscription_type": "basic-monthly",
                "country": "DE",
                "event_ts": pd.Timestamp("2024-01-10", tz="UTC"),
                "event_type": "acquisition",
                "renewal_period": "monthly",
                "currency": "EUR",
                "amount": 120.0,
            },
            {
                "platform": "apfel",
                "customer_id": "c1",
                "subscription_type": "basic-monthly",
                "country": "DE",
                "event_ts": pd.Timestamp("2024-02-10", tz="UTC"),
                "event_type": "renewal",
                "renewal_period": "monthly",
                "currency": "EUR",
                "amount": 120.0,
            },
            {
                "platform": "apfel",
                "customer_id": "c1",
                "subscription_type": "basic-monthly",
                "country": "DE",
                "event_ts": pd.Timestamp("2024-03-05", tz="UTC"),
                "event_type": "cancellation",
                "renewal_period": "monthly",
                "currency": "EUR",
                "amount": 120.0,
            },
        ]
    )

    exchange_rates = pd.DataFrame(
        [
            {"date": "2024-01-01", "currency": "EUR", "rate_to_eur": 1.0},
            {"date": "2024-02-01", "currency": "EUR", "rate_to_eur": 1.0},
            {"date": "2024-03-01", "currency": "EUR", "rate_to_eur": 1.0},
        ]
    )

    mrr = build_mrr_report(silver, exchange_rates)

    # The subscription is active at the end of January and February,
    # but not at the end of March because it was cancelled on 2024-03-05.
    jan = mrr[mrr["report_month"] == pd.Timestamp("2024-01-01")]
    feb = mrr[mrr["report_month"] == pd.Timestamp("2024-02-01")]
    mar = mrr[mrr["report_month"] == pd.Timestamp("2024-03-01")]

    assert not jan.empty
    assert not feb.empty
    assert mar.empty

    assert jan["mrr_eur"].iloc[0] == pytest.approx(120.0)
    assert feb["mrr_eur"].iloc[0] == pytest.approx(120.0)


def test_validate_gold_report_happy_path_and_failure() -> None:
    valid_gold = pd.DataFrame(
        [
            {
                "report_month": pd.Timestamp("2024-01-01"),
                "platform": "apfel",
                "subscription_type": "basic-monthly",
                "country": "DE",
                "acquisitions": 1,
                "renewals": 0,
                "cancellations": 0,
                "mrr_eur": 100.0,
            }
        ]
    )

    # Should not raise
    validate_gold_report(valid_gold)

    invalid_gold = valid_gold.copy()
    invalid_gold.loc[0, "mrr_eur"] = -10.0

    with pytest.raises(ValueError):
        validate_gold_report(invalid_gold)
