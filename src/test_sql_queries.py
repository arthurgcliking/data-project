from __future__ import annotations

from pathlib import Path
import sqlite3

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "analytics.db"


QUERIES = {
    "New users by month": """
        SELECT
            strftime('%Y-%m', signup_date) AS signup_month,
            COUNT(*) AS new_users
        FROM customers
        GROUP BY 1
        ORDER BY 1;
    """,
    "Trial to paid conversion rate": """
        SELECT
            ROUND(AVG(CAST(converted_to_paid AS REAL)), 4) AS trial_to_paid_conversion_rate
        FROM subscriptions;
    """,
    "Churn by plan": """
        SELECT
            plan_type,
            ROUND(AVG(CASE WHEN subscription_status = 'cancelled' THEN 1.0 ELSE 0.0 END), 4) AS churn_share
        FROM subscriptions
        WHERE converted_to_paid = 1
        GROUP BY plan_type
        ORDER BY churn_share DESC;
    """,
    "Row counts by table": """
        SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
        UNION ALL
        SELECT 'subscriptions', COUNT(*) FROM subscriptions
        UNION ALL
        SELECT 'payments', COUNT(*) FROM payments
        UNION ALL
        SELECT 'product_events', COUNT(*) FROM product_events
        UNION ALL
        SELECT 'monthly_customer_activity', COUNT(*) FROM monthly_customer_activity;
    """,
    "Duplicate customers check": """
        SELECT
            COUNT(*) AS duplicate_customer_ids
        FROM (
            SELECT customer_id
            FROM customers
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        );
    """,
}


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"SQLite database not found at {DB_PATH}. Run `python src/load_to_sqlite.py` first."
        )

    pd.set_option("display.max_columns", 100)
    pd.set_option("display.width", 120)

    connection = sqlite3.connect(DB_PATH)
    try:
        for title, query in QUERIES.items():
            print(f"\n=== {title} ===")
            result = pd.read_sql_query(query, connection)
            print(result.to_string(index=False))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
