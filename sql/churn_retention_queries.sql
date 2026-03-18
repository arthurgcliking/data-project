-- Churn, retention and cohort queries for the SQLite MVP analytics layer.
-- All queries are written to remain easy to explain and adapt in interview.

-- 1. Global churn share among paid customers
-- Business question: what share of converted customers eventually cancelled?
SELECT
    ROUND(AVG(CASE WHEN subscription_status = 'cancelled' THEN 1.0 ELSE 0.0 END), 4) AS global_churn_share
FROM subscriptions
WHERE converted_to_paid = 1;


-- 2. Churn share by plan
-- Business question: do monthly subscribers churn more than annual subscribers?
SELECT
    plan_type,
    ROUND(AVG(CASE WHEN subscription_status = 'cancelled' THEN 1.0 ELSE 0.0 END), 4) AS churn_share
FROM subscriptions
WHERE converted_to_paid = 1
GROUP BY plan_type
ORDER BY churn_share DESC;


-- 3. Churn share by acquisition channel
-- Business question: which channels bring customers who churn more?
SELECT
    c.acquisition_channel,
    ROUND(AVG(CASE WHEN s.subscription_status = 'cancelled' THEN 1.0 ELSE 0.0 END), 4) AS churn_share
FROM subscriptions s
JOIN customers c
    ON c.customer_id = s.customer_id
WHERE s.converted_to_paid = 1
GROUP BY c.acquisition_channel
ORDER BY churn_share DESC;


-- 4. Churn share by activation status
-- Business question: do activated customers retain better after conversion?
WITH activation_flags AS (
    SELECT
        c.customer_id,
        MAX(
            CASE
                WHEN pe.event_type IN ('bank_account_connected', 'transaction_imported')
                     AND date(pe.event_date) BETWEEN date(c.signup_date) AND date(c.signup_date, '+6 day')
                THEN 1 ELSE 0
            END
        ) AS has_value_event,
        MAX(
            CASE
                WHEN pe.event_type = 'budget_created'
                     AND date(pe.event_date) BETWEEN date(c.signup_date) AND date(c.signup_date, '+6 day')
                THEN 1 ELSE 0
            END
        ) AS has_budget_created
    FROM customers c
    LEFT JOIN product_events pe
        ON pe.customer_id = c.customer_id
    GROUP BY c.customer_id
)
SELECT
    CASE
        WHEN af.has_value_event = 1 AND af.has_budget_created = 1 THEN 'activated'
        ELSE 'not_activated'
    END AS activation_status,
    ROUND(AVG(CASE WHEN s.subscription_status = 'cancelled' THEN 1.0 ELSE 0.0 END), 4) AS churn_share
FROM subscriptions s
JOIN activation_flags af
    ON af.customer_id = s.customer_id
WHERE s.converted_to_paid = 1
GROUP BY activation_status
ORDER BY churn_share DESC;


-- 5. Churn share by latest observed usage segment
-- Business question: which usage groups appear most at risk?
-- Approximation:
-- - uses the latest observed row in monthly_customer_activity
-- - this table does not contain explicit zero-activity months
WITH latest_usage AS (
    SELECT
        mca.customer_id,
        mca.usage_segment
    FROM monthly_customer_activity mca
    JOIN (
        SELECT
            customer_id,
            MAX(activity_month) AS latest_activity_month
        FROM monthly_customer_activity
        GROUP BY customer_id
    ) last_month
        ON last_month.customer_id = mca.customer_id
       AND last_month.latest_activity_month = mca.activity_month
)
SELECT
    lu.usage_segment,
    ROUND(AVG(CASE WHEN s.subscription_status = 'cancelled' THEN 1.0 ELSE 0.0 END), 4) AS churn_share,
    COUNT(*) AS paid_customers
FROM subscriptions s
JOIN latest_usage lu
    ON lu.customer_id = s.customer_id
WHERE s.converted_to_paid = 1
GROUP BY lu.usage_segment
ORDER BY churn_share DESC;


-- 6. Monthly churn rate
-- Business question: how does churn evolve month by month?
-- Approximation:
-- - cancellations during month M
-- - divided by customers active at the start of month M
WITH RECURSIVE months(month_start) AS (
    SELECT date('2025-01-01')
    UNION ALL
    SELECT date(month_start, '+1 month')
    FROM months
    WHERE month_start < date('2025-12-01')
),
active_base AS (
    SELECT
        m.month_start,
        COUNT(s.customer_id) AS active_paid_at_start
    FROM months m
    LEFT JOIN subscriptions s
        ON s.converted_to_paid = 1
       AND date(s.subscription_start_date) < date(m.month_start)
       AND (
           s.subscription_end_date IS NULL
           OR date(s.subscription_end_date) >= date(m.month_start)
       )
    GROUP BY m.month_start
),
cancellations AS (
    SELECT
        m.month_start,
        COUNT(s.customer_id) AS cancelled_in_month
    FROM months m
    LEFT JOIN subscriptions s
        ON s.converted_to_paid = 1
       AND s.subscription_end_date IS NOT NULL
       AND date(s.subscription_end_date) BETWEEN date(m.month_start) AND date(m.month_start, '+1 month', '-1 day')
    GROUP BY m.month_start
)
SELECT
    strftime('%Y-%m', a.month_start) AS month,
    a.active_paid_at_start,
    c.cancelled_in_month,
    ROUND(
        CASE
            WHEN a.active_paid_at_start = 0 THEN 0.0
            ELSE c.cancelled_in_month * 1.0 / a.active_paid_at_start
        END,
        4
    ) AS monthly_churn_rate
FROM active_base a
JOIN cancellations c
    ON c.month_start = a.month_start
ORDER BY a.month_start;


-- 7. Simple paid retention curve
-- Business question: how much of the paid base survives after N months?
-- Approximation:
-- - retention is checked against month-end cutoffs after conversion
WITH RECURSIVE offsets(month_offset) AS (
    SELECT 0
    UNION ALL
    SELECT month_offset + 1
    FROM offsets
    WHERE month_offset < 6
)
SELECT
    o.month_offset AS months_since_conversion,
    ROUND(
        AVG(
            CASE
                WHEN s.subscription_end_date IS NULL THEN 1.0
                WHEN date(s.subscription_end_date) > date(s.subscription_start_date, '+' || o.month_offset || ' month', 'start of month', '+1 month', '-1 day') THEN 1.0
                ELSE 0.0
            END
        ),
        4
    ) AS paid_retention_rate
FROM offsets o
CROSS JOIN subscriptions s
WHERE s.converted_to_paid = 1
GROUP BY o.month_offset
ORDER BY o.month_offset;


-- 8. Converted customer cohort retention
-- Business question: how do conversion cohorts retain over time?
-- Output format:
-- - one row per cohort month and month offset
-- - easy to reuse in Python, BI or a spreadsheet heatmap
WITH RECURSIVE offsets(month_offset) AS (
    SELECT 0
    UNION ALL
    SELECT month_offset + 1
    FROM offsets
    WHERE month_offset < 11
),
paid_base AS (
    SELECT
        customer_id,
        date(subscription_start_date, 'start of month') AS cohort_month,
        subscription_start_date,
        subscription_end_date
    FROM subscriptions
    WHERE converted_to_paid = 1
)
SELECT
    strftime('%Y-%m', pb.cohort_month) AS cohort_month,
    o.month_offset,
    COUNT(*) AS cohort_size,
    SUM(
        CASE
            WHEN pb.subscription_end_date IS NULL THEN 1
            WHEN date(pb.subscription_end_date) > date(pb.subscription_start_date, '+' || o.month_offset || ' month', 'start of month', '+1 month', '-1 day') THEN 1
            ELSE 0
        END
    ) AS retained_customers,
    ROUND(
        AVG(
            CASE
                WHEN pb.subscription_end_date IS NULL THEN 1.0
                WHEN date(pb.subscription_end_date) > date(pb.subscription_start_date, '+' || o.month_offset || ' month', 'start of month', '+1 month', '-1 day') THEN 1.0
                ELSE 0.0
            END
        ),
        4
    ) AS retention_rate
FROM paid_base pb
JOIN offsets o
    ON date(pb.cohort_month, '+' || o.month_offset || ' month') <= date('2025-12-31')
GROUP BY pb.cohort_month, o.month_offset
ORDER BY pb.cohort_month, o.month_offset;


-- 9. First behavioral signals: churned vs retained
-- Business question: what simple usage differences exist between churned and retained paid customers?
-- Approximation:
-- - based on observed months present in monthly_customer_activity
WITH usage_summary AS (
    SELECT
        customer_id,
        COUNT(DISTINCT activity_month) AS observed_months,
        AVG(nb_app_opens) AS avg_app_opens,
        AVG(nb_transactions_imported) AS avg_transactions_imported,
        AVG(nb_dashboard_views) AS avg_dashboard_views
    FROM monthly_customer_activity
    GROUP BY customer_id
)
SELECT
    CASE
        WHEN s.subscription_status = 'cancelled' THEN 'churned'
        ELSE 'retained'
    END AS customer_status,
    COUNT(*) AS paid_customers,
    ROUND(AVG(COALESCE(us.observed_months, 0)), 2) AS avg_observed_months,
    ROUND(AVG(COALESCE(us.avg_app_opens, 0)), 2) AS avg_app_opens,
    ROUND(AVG(COALESCE(us.avg_transactions_imported, 0)), 2) AS avg_transactions_imported,
    ROUND(AVG(COALESCE(us.avg_dashboard_views, 0)), 2) AS avg_dashboard_views
FROM subscriptions s
LEFT JOIN usage_summary us
    ON us.customer_id = s.customer_id
WHERE s.converted_to_paid = 1
GROUP BY customer_status
ORDER BY customer_status DESC;


-- 10. Highest-risk segment combinations
-- Business question: which combinations of channel, plan and usage look riskiest?
WITH latest_usage AS (
    SELECT
        mca.customer_id,
        mca.usage_segment
    FROM monthly_customer_activity mca
    JOIN (
        SELECT
            customer_id,
            MAX(activity_month) AS latest_activity_month
        FROM monthly_customer_activity
        GROUP BY customer_id
    ) last_month
        ON last_month.customer_id = mca.customer_id
       AND last_month.latest_activity_month = mca.activity_month
)
SELECT
    c.acquisition_channel,
    s.plan_type,
    COALESCE(lu.usage_segment, 'no_observed_activity') AS usage_segment,
    COUNT(*) AS paid_customers,
    ROUND(AVG(CASE WHEN s.subscription_status = 'cancelled' THEN 1.0 ELSE 0.0 END), 4) AS churn_share
FROM subscriptions s
JOIN customers c
    ON c.customer_id = s.customer_id
LEFT JOIN latest_usage lu
    ON lu.customer_id = s.customer_id
WHERE s.converted_to_paid = 1
GROUP BY c.acquisition_channel, s.plan_type, COALESCE(lu.usage_segment, 'no_observed_activity')
HAVING COUNT(*) >= 5
ORDER BY churn_share DESC, paid_customers DESC;
