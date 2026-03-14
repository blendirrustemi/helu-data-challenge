from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def save_parquet(df: pd.DataFrame, path: str) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)


def save_to_duckdb(df: pd.DataFrame, table_name: str, db_path: str) -> None:
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(db_file)) as conn:
        conn.register("temp_df", df)
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
