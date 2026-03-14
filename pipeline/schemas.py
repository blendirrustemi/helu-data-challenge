"""
This module defines the schemas for the silver and gold tables, as well as the
column mappings for the Apfel and Fenster data sources.
"""

SILVER_COLUMNS = [
    "platform",
    "event_id",
    "event_ts",
    "event_type",
    "customer_id",
    "customer_email",
    "customer_created_at",
    "country",
    "region",
    "postal_code",
    "subscription_type",
    "renewal_period",
    "currency",
    "amount",
    "tax_amount",
]

GOLD_COLUMNS = [
    "report_month",
    "platform",
    "subscription_type",
    "country",
    "acquisitions",
    "renewals",
    "cancellations",
    "mrr_eur",
]

APFEL_COLUMN_MAP = {
    "event_id": "event_id",
    "event_timestamp": "event_ts",
    "event_type": "event_type",
    "customer_uuid": "customer_id",
    "customer_email": "customer_email",
    "customer_created_at": "customer_created_at",
    "country_code": "country",
    "region": "region",
    "postal_code": "postal_code",
    "subscription_type": "subscription_type",
    "renewal_period": "renewal_period",
    "currency": "currency",
    "amount": "amount",
    "tax_amount": "tax_amount",
}

FENSTER_COLUMN_MAP = {
    "id": "event_id",
    "ts": "event_ts",
    "type": "event_type",
    "cid": "customer_id",
    "mail": "customer_email",
    "signup_ts": "customer_created_at",
    "ctry": "country",
    "state": "region",
    "zip": "postal_code",
    "plan": "subscription_type",
    "ccy": "currency",
    "price": "amount",
    "tax": "tax_amount",
}
