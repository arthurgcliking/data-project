-- Data quality checks for the SQLite MVP analytics layer.
-- These checks are intentionally simple and useful for quick validation before analysis.

-- 1. Row counts by table
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL
SELECT 'subscriptions', COUNT(*) FROM subscriptions
UNION ALL
SELECT 'payments', COUNT(*) FROM payments
UNION ALL
SELECT 'product_events', COUNT(*) FROM product_events
UNION ALL
SELECT 'monthly_customer_activity', COUNT(*) FROM monthly_customer_activity;


-- 2. Duplicate primary key checks
SELECT customer_id, COUNT(*) AS duplicate_count
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT subscription_id, COUNT(*) AS duplicate_count
FROM subscriptions
GROUP BY subscription_id
HAVING COUNT(*) > 1;

SELECT payment_id, COUNT(*) AS duplicate_count
FROM payments
GROUP BY payment_id
HAVING COUNT(*) > 1;

SELECT event_id, COUNT(*) AS duplicate_count
FROM product_events
GROUP BY event_id
HAVING COUNT(*) > 1;

SELECT customer_id, activity_month, COUNT(*) AS duplicate_count
FROM monthly_customer_activity
GROUP BY customer_id, activity_month
HAVING COUNT(*) > 1;


-- 3. Missing-value checks on critical fields
SELECT COUNT(*) AS customers_missing_signup_date
FROM customers
WHERE signup_date IS NULL;

SELECT COUNT(*) AS subscriptions_missing_plan_type
FROM subscriptions
WHERE plan_type IS NULL;

SELECT COUNT(*) AS converted_subscriptions_missing_start_date
FROM subscriptions
WHERE converted_to_paid = 1
  AND subscription_start_date IS NULL;

SELECT COUNT(*) AS payments_missing_payment_date
FROM payments
WHERE payment_date IS NULL;

SELECT COUNT(*) AS product_events_missing_event_date
FROM product_events
WHERE event_date IS NULL;


-- 4. Chronology checks
SELECT COUNT(*) AS events_before_signup
FROM product_events pe
JOIN customers c
    ON c.customer_id = pe.customer_id
WHERE date(pe.event_date) < date(c.signup_date);

SELECT COUNT(*) AS payments_before_paid_start
FROM payments p
JOIN subscriptions s
    ON s.subscription_id = p.subscription_id
WHERE s.subscription_start_date IS NOT NULL
  AND date(p.payment_date) < date(s.subscription_start_date);

SELECT COUNT(*) AS converted_without_paid_start
FROM subscriptions
WHERE converted_to_paid = 1
  AND subscription_start_date IS NULL;

SELECT COUNT(*) AS non_converted_with_paid_start
FROM subscriptions
WHERE converted_to_paid = 0
  AND subscription_start_date IS NOT NULL;


-- 5. Status consistency checks
SELECT COUNT(*) AS paid_customers_without_paid_payment
FROM subscriptions s
WHERE s.converted_to_paid = 1
  AND NOT EXISTS (
      SELECT 1
      FROM payments p
      WHERE p.subscription_id = s.subscription_id
        AND p.payment_status = 'paid'
  );

SELECT COUNT(*) AS trial_only_with_payments
FROM subscriptions s
WHERE s.subscription_status = 'trial_only'
  AND EXISTS (
      SELECT 1
      FROM payments p
      WHERE p.subscription_id = s.subscription_id
  );


-- 6. Aggregated activity consistency check
-- Business question: does monthly_customer_activity align with product_events after aggregation?
WITH events_agg AS (
    SELECT
        customer_id,
        date(event_date, 'start of month') AS activity_month,
        SUM(CASE WHEN event_type = 'app_open' THEN 1 ELSE 0 END) AS nb_app_opens,
        SUM(CASE WHEN event_type = 'transaction_imported' THEN 1 ELSE 0 END) AS nb_transactions_imported,
        SUM(CASE WHEN event_type = 'budget_created' THEN 1 ELSE 0 END) AS nb_budgets_created,
        SUM(CASE WHEN event_type = 'saving_goal_created' THEN 1 ELSE 0 END) AS nb_goals_created,
        SUM(CASE WHEN event_type = 'dashboard_viewed' THEN 1 ELSE 0 END) AS nb_dashboard_views
    FROM product_events
    GROUP BY customer_id, date(event_date, 'start of month')
)
SELECT COUNT(*) AS inconsistent_monthly_activity_rows
FROM monthly_customer_activity mca
JOIN events_agg ea
    ON ea.customer_id = mca.customer_id
   AND ea.activity_month = mca.activity_month
WHERE ea.nb_app_opens != mca.nb_app_opens
   OR ea.nb_transactions_imported != mca.nb_transactions_imported
   OR ea.nb_budgets_created != mca.nb_budgets_created
   OR ea.nb_goals_created != mca.nb_goals_created
   OR ea.nb_dashboard_views != mca.nb_dashboard_views;
