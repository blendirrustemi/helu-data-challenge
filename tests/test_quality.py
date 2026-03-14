import pandas as pd

from pipeline.quality import (
    clean_silver_events,
    infer_renewal_period,
    normalize_event_type,
    normalize_country,
)


def test_normalize_event_type_mapping() -> None:
    assert normalize_event_type("SUBSCRIPTION_STARTED") == "acquisition"
    assert normalize_event_type("subscription_renewed") == "renewal"
    assert normalize_event_type("subscription_cancelled") == "cancellation"
    assert normalize_event_type("NEW") == "acquisition"
    assert normalize_event_type("renew") == "renewal"
    assert normalize_event_type("cancel") == "cancellation"
    assert normalize_event_type("UNKNOWN") is None
    assert normalize_event_type(None) is None


def test_infer_renewal_period_from_field_and_type() -> None:
    row_explicit = pd.Series(
        {"renewal_period": "Monthly", "subscription_type": "Pro Plan"}
    )
    assert infer_renewal_period(row_explicit) == "monthly"

    row_yearly_type = pd.Series(
        {"renewal_period": None, "subscription_type": "Yearly Basic"}
    )
    assert infer_renewal_period(row_yearly_type) == "yearly"

    row_unknown = pd.Series({"renewal_period": None, "subscription_type": "One-off"})
    assert infer_renewal_period(row_unknown) is None


def test_normalize_country_maps_known_variants() -> None:
    series = pd.Series(["Germany", "GBR", "United States", "", "INVALID", "XX"])
    normalized = normalize_country(series)

    assert normalized.iloc[0] == "DE"
    assert normalized.iloc[1] == "GB"
    assert normalized.iloc[2] == "US"
    assert pd.isna(normalized.iloc[3])
    assert pd.isna(normalized.iloc[4])
    assert pd.isna(normalized.iloc[5])


def test_clean_silver_events_drops_invalid_and_deduplicates() -> None:
    raw = pd.DataFrame(
        [
            # Valid acquisition
            {
                "platform": "apfel",
                "event_id": "e1",
                "event_ts": pd.Timestamp("2024-01-10", tz="UTC"),
                "event_type": "SUBSCRIPTION_STARTED",
                "customer_id": "c1",
                "country": "Germany",
                "subscription_type": "basic-monthly",
                "currency": "EUR",
                "amount": "10.0",
                "tax_amount": "2.0",
            },
            # Duplicate with same platform + event_id
            {
                "platform": "apfel",
                "event_id": "e1",
                "event_ts": pd.Timestamp("2024-01-11", tz="UTC"),
                "event_type": "SUBSCRIPTION_STARTED",
                "customer_id": "c1",
                "country": "Germany",
                "subscription_type": "basic-monthly",
                "currency": "EUR",
                "amount": "10.0",
                "tax_amount": "2.0",
            },
            # Invalid event type
            {
                "platform": "apfel",
                "event_id": "e2",
                "event_ts": pd.Timestamp("2024-01-12", tz="UTC"),
                "event_type": "UNKNOWN",
                "customer_id": "c2",
                "country": "Germany",
                "subscription_type": "basic-monthly",
                "currency": "EUR",
                "amount": "10.0",
                "tax_amount": "2.0",
            },
            # Missing required field (amount)
            {
                "platform": "apfel",
                "event_id": "e3",
                "event_ts": pd.Timestamp("2024-01-13", tz="UTC"),
                "event_type": "SUBSCRIPTION_STARTED",
                "customer_id": "c3",
                "country": "Germany",
                "subscription_type": "basic-monthly",
                "currency": "EUR",
                "amount": None,
                "tax_amount": "2.0",
            },
        ]
    )

    cleaned = clean_silver_events(raw)

    # Only one valid, non-duplicate row should remain (e1)
    assert len(cleaned) == 1
    assert cleaned.iloc[0]["event_id"] == "e1"
    assert cleaned.iloc[0]["event_type"] == "acquisition"
