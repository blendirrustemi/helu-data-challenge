from __future__ import annotations

import logging
import os
from pathlib import Path

from pipeline.extract import (
    fetch_apfel_subscriptions,
    fetch_exchange_rates,
    fetch_fenster_subscriptions,
)
from pipeline.transform import (
    combine_silver_events,
    transform_apfel_to_silver,
    transform_fenster_to_silver,
)
from pipeline.quality import clean_silver_events
from pipeline.report import build_gold_report
from pipeline.load import save_parquet, save_to_duckdb
from pipeline.validation import validate_gold_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output"


def run_pipeline(base_url: str | None = None) -> None:
    """
    Run the pipeline end to end.

    The base_url can be provided explicitly or via the SUBSCRIPTION_API_URL
    environment variable. If neither is set, it defaults to http://localhost:5050.
    """
    if base_url is None:
        base_url = os.environ.get("SUBSCRIPTION_API_URL", "http://localhost:5050")

    logger.info("Using subscription API at %s", base_url)

    logger.info("Fetching source data from API")
    apfel_raw = fetch_apfel_subscriptions(base_url)
    fenster_raw = fetch_fenster_subscriptions(base_url)
    exchange_rates = fetch_exchange_rates(base_url)

    logger.info("Fetched %s Apfel rows", len(apfel_raw))
    logger.info("Fetched %s Fenster rows", len(fenster_raw))
    logger.info("Fetched %s exchange-rate rows", len(exchange_rates))

    logger.info("Transforming raw subscription data into Silver schema")
    apfel_silver = transform_apfel_to_silver(apfel_raw)
    fenster_silver = transform_fenster_to_silver(fenster_raw)

    silver_events = combine_silver_events(apfel_silver, fenster_silver)
    logger.info("Combined Silver rows before cleaning: %s", len(silver_events))

    silver_events = clean_silver_events(silver_events)
    logger.info("Silver rows after cleaning: %s", len(silver_events))

    gold_report = build_gold_report(silver_events, exchange_rates)
    logger.info("Built gold report with %s rows", len(gold_report))

    validate_gold_report(gold_report)
    logger.info("Gold report passed validation checks")

    save_parquet(silver_events, str(OUTPUT_DIR / "silver_subscription_events.parquet"))
    save_parquet(gold_report, str(OUTPUT_DIR / "gold_monthly_report.parquet"))

    save_to_duckdb(
        silver_events,
        "silver_subscription_events",
        str(OUTPUT_DIR / "warehouse.duckdb"),
    )
    save_to_duckdb(
        gold_report,
        "gold_monthly_report",
        str(OUTPUT_DIR / "warehouse.duckdb"),
    )

    logger.info(f"Saved Silver and Gold outputs to Parquet and DuckDB to: {OUTPUT_DIR}")


def main() -> None:
    run_pipeline()


if __name__ == "__main__":
    main()
