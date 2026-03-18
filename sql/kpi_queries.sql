-- KPI queries for the SQLite MVP analytics layer.
-- These queries favor clarity over sophistication so they stay easy to explain in interview.

-- 1. New users by month
-- Business question: how many users joined the product each month?
SELECT
    strftime('%Y-%m', signup_date) AS signup_month,
    COUNT(*) AS new_users
FROM customers
GROUP BY 1
ORDER BY 1;


-- 2. Trial to paid conversion rate
-- Business question: what share of trial users became paying subscribers?
SELECT
    ROUND(AVG(CAST(converted_to_paid AS REAL)), 4) AS trial_to_paid_conversion_rate
FROM subscriptions;


-- 3. Activation rate
-- Business question: what share of users completed the activation journey?
-- Activation definition:
-- - within 7 days after signup
-- - at least one bank_account_connected or transaction_imported
-- - at least one budget_created
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
    ROUND(AVG(CASE WHEN has_value_event = 1 AND has_budget_created = 1 THEN 1.0 ELSE 0.0 END), 4) AS activation_rate
FROM activation_flags;


-- 4. Activation rate by acquisition channel
-- Business question: which channels bring users who activate better?
WITH activation_flags AS (
    SELECT
        c.customer_id,
        c.acquisition_channel,
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
    GROUP BY c.customer_id, c.acquisition_channel
)
SELECT
    acquisition_channel,
    ROUND(AVG(CASE WHEN has_value_event = 1 AND has_budget_created = 1 THEN 1.0 ELSE 0.0 END), 4) AS activation_rate
FROM activation_flags
GROUP BY acquisition_channel
ORDER BY activation_rate DESC;


-- 5. Trial to paid conversion rate by acquisition channel
-- Business question: which channels convert best from trial to paid?
SELECT
    c.acquisition_channel,
    ROUND(AVG(CAST(s.converted_to_paid AS REAL)), 4) AS conversion_rate
FROM subscriptions s
JOIN customers c
    ON c.customer_id = s.customer_id
GROUP BY c.acquisition_channel
ORDER BY conversion_rate DESC;


-- 6. Conversion rate for activated vs non-activated users
-- Business question: how much does activation improve monetization?
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
        WHEN has_value_event = 1 AND has_budget_created = 1 THEN 'activated'
        ELSE 'not_activated'
    END AS activation_status,
    ROUND(AVG(CAST(s.converted_to_paid AS REAL)), 4) AS conversion_rate
FROM activation_flags af
JOIN subscriptions s
    ON s.customer_id = af.customer_id
GROUP BY activation_status
ORDER BY conversion_rate DESC;


-- 7. Plan mix among paid customers
-- Business question: what is the split between monthly and annual paid subscriptions?
SELECT
    plan_type,
    COUNT(*) AS paid_customers,
    ROUND(COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (), 4) AS paid_customer_share
FROM subscriptions
WHERE converted_to_paid = 1
GROUP BY plan_type
ORDER BY paid_customer_share DESC;


-- 8. Simple ARPU
-- Business question: how much revenue does a paying customer generate on average?
-- Approximation:
-- - based on paid payment records present in the dataset
SELECT
    ROUND(
        SUM(CASE WHEN payment_status = 'paid' THEN amount ELSE 0 END) * 1.0
        / COUNT(DISTINCT CASE WHEN payment_status = 'paid' THEN customer_id END),
        2
    ) AS simple_arpu
FROM payments;


-- 9. End-of-period MRR
-- Business question: what is the approximate monthly recurring revenue at the end of the observed period?
-- Approximation:
-- - active paid subscriptions at 2025-12-31
-- - annual plans already normalized through monthly_price
SELECT
    ROUND(SUM(monthly_price), 2) AS approximate_end_of_period_mrr,
    COUNT(*) AS active_paid_customers
FROM subscriptions
WHERE converted_to_paid = 1
  AND date(subscription_start_date) <= date('2025-12-31')
  AND (
      subscription_end_date IS NULL
      OR date(subscription_end_date) > date('2025-12-31')
  );


-- 10. Monthly MRR evolution
-- Business question: how does approximate MRR evolve month by month?
WITH RECURSIVE months(month_start) AS (
    SELECT date('2025-01-01')
    UNION ALL
    SELECT date(month_start, '+1 month')
    FROM months
    WHERE month_start < date('2025-12-01')
)
SELECT
    strftime('%Y-%m', m.month_start) AS month,
    COUNT(s.subscription_id) AS active_paid_customers,
    ROUND(COALESCE(SUM(s.monthly_price), 0), 2) AS approximate_mrr
FROM months m
LEFT JOIN subscriptions s
    ON s.converted_to_paid = 1
   AND date(s.subscription_start_date) <= date(m.month_start, 'start of month', '+1 month', '-1 day')
   AND (
       s.subscription_end_date IS NULL
       OR date(s.subscription_end_date) > date(m.month_start, 'start of month', '+1 month', '-1 day')
   )
GROUP BY m.month_start
ORDER BY m.month_start;
