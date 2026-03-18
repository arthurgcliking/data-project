-- SQLite schema for the MVP SaaS subscription analytics project.
-- Dates are stored as ISO-8601 text values to stay compatible with SQLite date functions.

DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    signup_date TEXT NOT NULL,
    country TEXT NOT NULL,
    acquisition_channel TEXT NOT NULL,
    device_type TEXT NOT NULL,
    age_group TEXT NOT NULL
);

DROP TABLE IF EXISTS subscriptions;
CREATE TABLE subscriptions (
    subscription_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    plan_type TEXT NOT NULL,
    trial_start_date TEXT NOT NULL,
    trial_end_date TEXT NOT NULL,
    converted_to_paid INTEGER NOT NULL,
    subscription_start_date TEXT,
    subscription_end_date TEXT,
    subscription_status TEXT NOT NULL,
    monthly_price REAL NOT NULL,
    billing_cycle_months INTEGER NOT NULL,
    cancel_reason TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

DROP TABLE IF EXISTS payments;
CREATE TABLE payments (
    payment_id TEXT PRIMARY KEY,
    subscription_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    payment_date TEXT NOT NULL,
    amount REAL NOT NULL,
    payment_status TEXT NOT NULL,
    payment_type TEXT NOT NULL,
    FOREIGN KEY (subscription_id) REFERENCES subscriptions(subscription_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

DROP TABLE IF EXISTS product_events;
CREATE TABLE product_events (
    event_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    event_date TEXT NOT NULL,
    event_type TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

DROP TABLE IF EXISTS monthly_customer_activity;
CREATE TABLE monthly_customer_activity (
    customer_id TEXT NOT NULL,
    activity_month TEXT NOT NULL,
    nb_app_opens INTEGER NOT NULL,
    nb_transactions_imported INTEGER NOT NULL,
    nb_budgets_created INTEGER NOT NULL,
    nb_goals_created INTEGER NOT NULL,
    nb_dashboard_views INTEGER NOT NULL,
    is_active_month INTEGER NOT NULL,
    usage_segment TEXT NOT NULL,
    PRIMARY KEY (customer_id, activity_month),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
