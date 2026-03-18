from __future__ import annotations

from pathlib import Path
import sqlite3

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "raw"
DB_PATH = PROJECT_ROOT / "data" / "analytics.db"
SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"

TABLE_FILES = {
    "customers": "customers.csv",
    "subscriptions": "subscriptions.csv",
    "payments": "payments.csv",
    "product_events": "product_events.csv",
    "monthly_customer_activity": "monthly_customer_activity.csv",
}


def load_csv_tables() -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    for table_name, filename in TABLE_FILES.items():
        csv_path = DATA_DIR / filename
        tables[table_name] = pd.read_csv(csv_path)
    return tables


def recreate_database() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    connection = sqlite3.connect(DB_PATH)
    schema_sql = SCHEMA_PATH.read_text()
    connection.executescript(schema_sql)
    return connection


def load_tables_to_sqlite(connection: sqlite3.Connection, tables: dict[str, pd.DataFrame]) -> dict[str, int]:
    row_counts: dict[str, int] = {}

    for table_name, frame in tables.items():
        frame.to_sql(table_name, connection, if_exists="append", index=False)
        row_counts[table_name] = len(frame)

    connection.commit()
    return row_counts


def main() -> None:
    tables = load_csv_tables()
    connection = recreate_database()

    try:
        row_counts = load_tables_to_sqlite(connection, tables)
    finally:
        connection.close()

    print(f"SQLite database created at: {DB_PATH}")
    for table_name, row_count in row_counts.items():
        print(f"{table_name}: {row_count:,} rows loaded")


if __name__ == "__main__":
    main()
