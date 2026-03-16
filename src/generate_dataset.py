from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple
import random

import pandas as pd


RANDOM_SEED = 42
N_CUSTOMERS = 4000
DATASET_START = pd.Timestamp("2025-01-01")
DATASET_END = pd.Timestamp("2025-12-31")
TRIAL_LENGTH_DAYS = 14

COUNTRY_WEIGHTS = {
    "France": 0.55,
    "Belgium": 0.15,
    "Switzerland": 0.10,
    "Canada": 0.10,
    "Spain": 0.10,
}

CHANNEL_WEIGHTS = {
    "Organic": 0.35,
    "Paid Social": 0.25,
    "Search Ads": 0.20,
    "Referral": 0.15,
    "Partner": 0.05,
}

AGE_GROUP_WEIGHTS = {
    "18-24": 0.20,
    "25-34": 0.40,
    "35-44": 0.25,
    "45+": 0.15,
}

CHANNEL_DEVICE_WEIGHTS = {
    "Organic": {"mobile": 0.70, "desktop": 0.30},
    "Paid Social": {"mobile": 0.88, "desktop": 0.12},
    "Search Ads": {"mobile": 0.68, "desktop": 0.32},
    "Referral": {"mobile": 0.72, "desktop": 0.28},
    "Partner": {"mobile": 0.60, "desktop": 0.40},
}

CHANNEL_ACTIVATION_BONUS = {
    "Organic": 0.05,
    "Paid Social": -0.08,
    "Search Ads": 0.00,
    "Referral": 0.08,
    "Partner": -0.02,
}

CHANNEL_CONVERSION_BONUS = {
    "Organic": 0.04,
    "Paid Social": -0.06,
    "Search Ads": 0.00,
    "Referral": 0.06,
    "Partner": -0.01,
}

CHANNEL_CHURN_BONUS = {
    "Organic": -0.01,
    "Paid Social": 0.02,
    "Search Ads": 0.00,
    "Referral": -0.02,
    "Partner": 0.00,
}

MONTHLY_PRICE = 9.99
ANNUAL_PRICE = 99.96
ANNUAL_MONTHLY_EQUIVALENT = 8.33

EVENT_TYPES = [
    "app_open",
    "bank_account_connected",
    "transaction_imported",
    "budget_created",
    "saving_goal_created",
    "dashboard_viewed",
]

TARGETS = {
    "activation_rate": (0.52, 0.68),
    "conversion_rate": (0.20, 0.30),
    "annual_share": (0.18, 0.32),
    "monthly_churn": (0.04, 0.09),
}


@dataclass
class CustomerSignals:
    activated: bool
    early_usage_score: float
    converted: bool
    initial_usage_segment: str
    will_churn: bool
    monthly_churn_rate: float | None


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def weighted_choice(rng: random.Random, weights: Dict[str, float]) -> str:
    population = list(weights.keys())
    values = list(weights.values())
    return rng.choices(population, weights=values, k=1)[0]


def daterange_month_starts(start: pd.Timestamp, end: pd.Timestamp) -> List[pd.Timestamp]:
    return list(pd.date_range(start=start, end=end, freq="MS"))


def generate_customers(rng: random.Random) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    signup_days = pd.date_range(DATASET_START, DATASET_END, freq="D")

    for idx in range(1, N_CUSTOMERS + 1):
        signup_date = signup_days[rng.randrange(len(signup_days))]
        acquisition_channel = weighted_choice(rng, CHANNEL_WEIGHTS)
        rows.append(
            {
                "customer_id": f"CUST_{idx:06d}",
                "signup_date": signup_date.normalize(),
                "country": weighted_choice(rng, COUNTRY_WEIGHTS),
                "acquisition_channel": acquisition_channel,
                "device_type": weighted_choice(rng, CHANNEL_DEVICE_WEIGHTS[acquisition_channel]),
                "age_group": weighted_choice(rng, AGE_GROUP_WEIGHTS),
            }
        )

    customers = pd.DataFrame(rows).sort_values("signup_date").reset_index(drop=True)
    return customers


def generate_subscriptions(
    customers: pd.DataFrame, rng: random.Random
) -> Tuple[pd.DataFrame, Dict[str, CustomerSignals]]:
    rows: List[Dict[str, object]] = []
    customer_signals: Dict[str, CustomerSignals] = {}

    for idx, customer in enumerate(customers.itertuples(index=False), start=1):
        signup_date = pd.Timestamp(customer.signup_date)
        trial_start_date = signup_date
        trial_end_date = signup_date + pd.Timedelta(days=TRIAL_LENGTH_DAYS)

        activation_prob = 0.58 + CHANNEL_ACTIVATION_BONUS[customer.acquisition_channel]
        if customer.device_type == "mobile":
            activation_prob += 0.02
        if customer.age_group == "25-34":
            activation_prob += 0.01
        activation_prob += rng.uniform(-0.08, 0.08)
        activated = rng.random() < clamp(activation_prob, 0.10, 0.92)

        early_usage_score = rng.uniform(0.45, 1.00) if activated else rng.uniform(0.00, 0.55)

        conversion_prob = 0.04 + CHANNEL_CONVERSION_BONUS[customer.acquisition_channel]
        if activated:
            conversion_prob += 0.18
        conversion_prob += 0.11 * early_usage_score
        if customer.device_type == "desktop":
            conversion_prob += 0.01
        conversion_prob += rng.uniform(-0.05, 0.05)
        converted = rng.random() < clamp(conversion_prob, 0.01, 0.90)
        earliest_paid_start = trial_end_date + pd.Timedelta(days=1)

        # A customer cannot convert within the observed dataset if paid access starts after the dataset end.
        if earliest_paid_start > DATASET_END:
            converted = False

        if converted:
            annual_prob = 0.18
            if activated:
                annual_prob += 0.06
            if early_usage_score > 0.75:
                annual_prob += 0.05
            if customer.acquisition_channel in {"Organic", "Referral"}:
                annual_prob += 0.04
            annual_prob += rng.uniform(-0.03, 0.03)
            plan_type = "annual" if rng.random() < clamp(annual_prob, 0.05, 0.65) else "monthly"
            subscription_start_date = earliest_paid_start

            base_churn_prob = 0.48 if plan_type == "monthly" else 0.08
            base_churn_prob -= 0.16 if activated else 0.0
            base_churn_prob -= 0.10 if early_usage_score > 0.75 else 0.0
            base_churn_prob += CHANNEL_CHURN_BONUS[customer.acquisition_channel]
            base_churn_prob += rng.uniform(-0.06, 0.06)
            will_churn = rng.random() < clamp(base_churn_prob, 0.02, 0.80)

            if early_usage_score >= 0.80:
                initial_usage_segment = "power"
            elif early_usage_score >= 0.45:
                initial_usage_segment = "medium"
            else:
                initial_usage_segment = "low"

            if will_churn:
                if plan_type == "monthly":
                    max_months = max(1, ((DATASET_END.to_period("M") - subscription_start_date.to_period("M")).n))
                    survival_months = rng.randint(1, max(1, min(max_months, 8)))
                    subscription_end_date = (
                        subscription_start_date + pd.DateOffset(months=survival_months)
                    ).normalize()
                    if subscription_end_date > DATASET_END:
                        subscription_end_date = DATASET_END
                        will_churn = False
                        subscription_status = "active"
                        cancel_reason = None
                    else:
                        subscription_status = "cancelled"
                        cancel_reason = weighted_choice(
                            rng,
                            {
                                "too_expensive": 0.35,
                                "low_value": 0.40,
                                "switched_tool": 0.15,
                                "temporary_pause": 0.10,
                            },
                        )
                else:
                    annual_end = (subscription_start_date + pd.DateOffset(years=1)).normalize()
                    if annual_end <= DATASET_END and rng.random() < 0.40:
                        subscription_end_date = annual_end
                        subscription_status = "cancelled"
                        cancel_reason = weighted_choice(
                            rng,
                            {
                                "too_expensive": 0.30,
                                "low_value": 0.45,
                                "switched_tool": 0.15,
                                "temporary_pause": 0.10,
                            },
                        )
                    else:
                        subscription_end_date = pd.NaT
                        subscription_status = "active"
                        cancel_reason = None
                        will_churn = False
            else:
                subscription_end_date = pd.NaT
                subscription_status = "active"
                cancel_reason = None

            monthly_price = ANNUAL_MONTHLY_EQUIVALENT if plan_type == "annual" else MONTHLY_PRICE
            billing_cycle_months = 12 if plan_type == "annual" else 1
            monthly_churn_rate = 0.065 if plan_type == "monthly" and will_churn else None
        else:
            plan_type = "monthly"
            subscription_start_date = pd.NaT
            subscription_end_date = pd.NaT
            subscription_status = "trial_only"
            monthly_price = MONTHLY_PRICE
            billing_cycle_months = 1
            cancel_reason = None
            will_churn = False
            initial_usage_segment = "low" if early_usage_score < 0.25 else "medium"
            monthly_churn_rate = None

        rows.append(
            {
                "subscription_id": f"SUB_{idx:06d}",
                "customer_id": customer.customer_id,
                "plan_type": plan_type,
                "trial_start_date": trial_start_date.normalize(),
                "trial_end_date": trial_end_date.normalize(),
                "converted_to_paid": converted,
                "subscription_start_date": subscription_start_date,
                "subscription_end_date": subscription_end_date,
                "subscription_status": subscription_status,
                "monthly_price": monthly_price,
                "billing_cycle_months": billing_cycle_months,
                "cancel_reason": cancel_reason,
            }
        )

        customer_signals[customer.customer_id] = CustomerSignals(
            activated=activated,
            early_usage_score=early_usage_score,
            converted=converted,
            initial_usage_segment=initial_usage_segment,
            will_churn=will_churn,
            monthly_churn_rate=monthly_churn_rate,
        )

    subscriptions = pd.DataFrame(rows)
    return subscriptions, customer_signals


def generate_payments(subscriptions: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    payment_idx = 1

    for subscription in subscriptions.itertuples(index=False):
        if not subscription.converted_to_paid:
            continue

        subscription_start = pd.Timestamp(subscription.subscription_start_date)
        end_date = (
            pd.Timestamp(subscription.subscription_end_date)
            if pd.notna(subscription.subscription_end_date)
            else DATASET_END
        )

        if subscription.plan_type == "monthly":
            payment_dates = list(pd.date_range(subscription_start, end_date, freq="MS"))
            if subscription_start.day != 1:
                payment_dates = [subscription_start] + [
                    date for date in payment_dates if date > subscription_start
                ]

            for order, payment_date in enumerate(payment_dates):
                if payment_date > end_date:
                    continue
                rows.append(
                    {
                        "payment_id": f"PAY_{payment_idx:06d}",
                        "subscription_id": subscription.subscription_id,
                        "customer_id": subscription.customer_id,
                        "payment_date": pd.Timestamp(payment_date).normalize(),
                        "amount": MONTHLY_PRICE,
                        "payment_status": "paid",
                        "payment_type": "trial_conversion" if order == 0 else "renewal",
                    }
                )
                payment_idx += 1
        else:
            rows.append(
                {
                    "payment_id": f"PAY_{payment_idx:06d}",
                    "subscription_id": subscription.subscription_id,
                    "customer_id": subscription.customer_id,
                    "payment_date": subscription_start.normalize(),
                    "amount": ANNUAL_PRICE,
                    "payment_status": "paid",
                    "payment_type": "trial_conversion",
                }
            )
            payment_idx += 1

    return pd.DataFrame(rows)


def add_event(rows: List[Dict[str, object]], event_idx: int, customer_id: str, event_date: pd.Timestamp, event_type: str) -> int:
    rows.append(
        {
            "event_id": f"EVT_{event_idx:07d}",
            "customer_id": customer_id,
            "event_date": event_date.normalize(),
            "event_type": event_type,
        }
    )
    return event_idx + 1


def month_bounds(month_start: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Timestamp]:
    month_end = month_start + pd.offsets.MonthEnd(1)
    return month_start, month_end.normalize()


def generate_product_events(
    customers: pd.DataFrame, subscriptions: pd.DataFrame, customer_signals: Dict[str, CustomerSignals], rng: random.Random
) -> pd.DataFrame:
    subscription_lookup = subscriptions.set_index("customer_id").to_dict("index")
    rows: List[Dict[str, object]] = []
    event_idx = 1

    for customer in customers.itertuples(index=False):
        subscription = subscription_lookup[customer.customer_id]
        signal = customer_signals[customer.customer_id]
        signup_date = pd.Timestamp(customer.signup_date)
        trial_end_date = pd.Timestamp(subscription["trial_end_date"])

        app_open_count = rng.randint(2, 5) if signal.activated else rng.randint(1, 3)
        for _ in range(app_open_count):
            event_date = signup_date + pd.Timedelta(days=rng.randint(0, min(13, (DATASET_END - signup_date).days)))
            event_idx = add_event(rows, event_idx, customer.customer_id, event_date, "app_open")

        dashboard_count = rng.randint(1, 4) if signal.activated else rng.randint(0, 2)
        for _ in range(dashboard_count):
            event_date = signup_date + pd.Timedelta(days=rng.randint(0, min(13, (DATASET_END - signup_date).days)))
            event_idx = add_event(rows, event_idx, customer.customer_id, event_date, "dashboard_viewed")

        if signal.activated:
            activation_day = signup_date + pd.Timedelta(days=rng.randint(0, 6))
            if rng.random() < 0.55:
                event_idx = add_event(rows, event_idx, customer.customer_id, activation_day, "bank_account_connected")
                import_day = activation_day + pd.Timedelta(days=rng.randint(0, 2))
                if import_day <= DATASET_END:
                    event_idx = add_event(rows, event_idx, customer.customer_id, import_day, "transaction_imported")
            else:
                event_idx = add_event(rows, event_idx, customer.customer_id, activation_day, "transaction_imported")
            budget_day = signup_date + pd.Timedelta(days=rng.randint(0, 6))
            event_idx = add_event(rows, event_idx, customer.customer_id, budget_day, "budget_created")
            if rng.random() < 0.45:
                goal_day = signup_date + pd.Timedelta(days=rng.randint(1, 10))
                if goal_day <= DATASET_END:
                    event_idx = add_event(rows, event_idx, customer.customer_id, goal_day, "saving_goal_created")
        else:
            if rng.random() < 0.15:
                late_day = signup_date + pd.Timedelta(days=rng.randint(8, 20))
                if late_day <= DATASET_END:
                    event_idx = add_event(rows, event_idx, customer.customer_id, late_day, "transaction_imported")

        if not signal.converted:
            continue

        subscription_start = pd.Timestamp(subscription["subscription_start_date"])
        end_date = (
            pd.Timestamp(subscription["subscription_end_date"])
            if pd.notna(subscription["subscription_end_date"])
            else DATASET_END
        )
        month_starts = daterange_month_starts(subscription_start.replace(day=1), end_date.replace(day=1))
        churn_month = end_date.replace(day=1) if signal.will_churn and pd.notna(subscription["subscription_end_date"]) else None

        for month_start in month_starts:
            month_begin, month_end = month_bounds(month_start)
            effective_start = max(month_begin, subscription_start)
            effective_end = min(month_end, end_date)
            if effective_start > effective_end:
                continue

            segment = signal.initial_usage_segment
            if segment == "power":
                base_opens = rng.randint(10, 18)
                base_transactions = rng.randint(3, 7)
                base_dashboards = rng.randint(6, 12)
                base_budgets = rng.randint(0, 2)
                base_goals = rng.randint(0, 2)
            elif segment == "medium":
                base_opens = rng.randint(5, 9)
                base_transactions = rng.randint(1, 4)
                base_dashboards = rng.randint(3, 7)
                base_budgets = rng.randint(0, 1)
                base_goals = rng.randint(0, 1)
            else:
                base_opens = rng.randint(2, 4)
                base_transactions = rng.randint(0, 2)
                base_dashboards = rng.randint(1, 3)
                base_budgets = rng.randint(0, 1)
                base_goals = 0

            if churn_month is not None:
                month_gap = (churn_month.to_period("M") - month_start.to_period("M")).n
                if month_gap == 1:
                    base_opens = max(1, base_opens // 2)
                    base_transactions = max(0, base_transactions - 1)
                    base_dashboards = max(1, base_dashboards // 2)
                    base_budgets = 0
                elif month_gap == 0:
                    base_opens = 1
                    base_transactions = 0
                    base_dashboards = 1
                    base_budgets = 0
                    base_goals = 0

            for _ in range(base_opens):
                event_date = effective_start + pd.Timedelta(
                    days=rng.randint(0, max((effective_end - effective_start).days, 0))
                )
                event_idx = add_event(rows, event_idx, customer.customer_id, event_date, "app_open")

            for _ in range(base_transactions):
                event_date = effective_start + pd.Timedelta(
                    days=rng.randint(0, max((effective_end - effective_start).days, 0))
                )
                event_idx = add_event(rows, event_idx, customer.customer_id, event_date, "transaction_imported")

            for _ in range(base_dashboards):
                event_date = effective_start + pd.Timedelta(
                    days=rng.randint(0, max((effective_end - effective_start).days, 0))
                )
                event_idx = add_event(rows, event_idx, customer.customer_id, event_date, "dashboard_viewed")

            for _ in range(base_budgets):
                event_date = effective_start + pd.Timedelta(
                    days=rng.randint(0, max((effective_end - effective_start).days, 0))
                )
                event_idx = add_event(rows, event_idx, customer.customer_id, event_date, "budget_created")

            for _ in range(base_goals):
                event_date = effective_start + pd.Timedelta(
                    days=rng.randint(0, max((effective_end - effective_start).days, 0))
                )
                event_idx = add_event(rows, event_idx, customer.customer_id, event_date, "saving_goal_created")

    events = pd.DataFrame(rows)
    events = events.sort_values(["event_date", "customer_id", "event_type"]).reset_index(drop=True)
    return events


def build_monthly_customer_activity(product_events: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    events = product_events.copy()
    events["activity_month"] = pd.to_datetime(events["event_date"]).values.astype("datetime64[M]")

    counts = (
        events.pivot_table(
            index=["customer_id", "activity_month"],
            columns="event_type",
            values="event_id",
            aggfunc="count",
            fill_value=0,
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )

    for column in EVENT_TYPES:
        if column not in counts.columns:
            counts[column] = 0

    counts["nb_app_opens"] = counts["app_open"]
    counts["nb_transactions_imported"] = counts["transaction_imported"]
    counts["nb_budgets_created"] = counts["budget_created"]
    counts["nb_goals_created"] = counts["saving_goal_created"]
    counts["nb_dashboard_views"] = counts["dashboard_viewed"]

    action_count = (
        counts["nb_transactions_imported"] + counts["nb_budgets_created"] + counts["nb_goals_created"]
    )
    counts["is_active_month"] = (
        (counts["nb_app_opens"] >= 2) | (action_count >= 1)
    )

    def derive_usage_segment(row: pd.Series) -> str:
        if row["nb_app_opens"] >= 10 or action_count.loc[row.name] >= 4:
            return "power"
        if 4 <= row["nb_app_opens"] <= 9 or 2 <= action_count.loc[row.name] <= 3:
            return "medium"
        return "low"

    counts["usage_segment"] = counts.apply(derive_usage_segment, axis=1)

    activity = counts[
        [
            "customer_id",
            "activity_month",
            "nb_app_opens",
            "nb_transactions_imported",
            "nb_budgets_created",
            "nb_goals_created",
            "nb_dashboard_views",
            "is_active_month",
            "usage_segment",
        ]
    ].sort_values(["customer_id", "activity_month"])

    activity["activity_month"] = pd.to_datetime(activity["activity_month"])
    return activity.reset_index(drop=True)


def run_quality_checks(
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    payments: pd.DataFrame,
    product_events: pd.DataFrame,
    monthly_customer_activity: pd.DataFrame,
) -> None:
    customer_signup = customers.set_index("customer_id")["signup_date"]
    event_signup = product_events["customer_id"].map(customer_signup)
    assert (product_events["event_date"] >= event_signup).all(), "Event found before signup_date."

    subscription_lookup = subscriptions.set_index("customer_id")
    converted_lookup = subscription_lookup["converted_to_paid"]
    assert payments["customer_id"].map(converted_lookup).all(), "Payment found for non-converted customer."

    paid_start_lookup = subscription_lookup["subscription_start_date"]
    payment_start_dates = payments["customer_id"].map(paid_start_lookup)
    assert (payments["payment_date"] >= payment_start_dates).all(), "Payment found before subscription start."

    non_converted = subscriptions[~subscriptions["converted_to_paid"]]
    assert non_converted["subscription_start_date"].isna().all(), "Non-converted customer has subscription_start_date."
    assert (non_converted["subscription_status"] == "trial_only").all(), "Non-converted customer has invalid status."

    converted = subscriptions[subscriptions["converted_to_paid"]]
    assert converted["subscription_start_date"].notna().all(), "Converted customer missing subscription_start_date."
    assert converted["subscription_status"].isin(["active", "cancelled"]).all(), "Converted customer has invalid status."

    converted_customers = set(converted["customer_id"])
    paid_customers = set(payments.loc[payments["payment_status"] == "paid", "customer_id"])
    converted_without_paid = converted_customers - paid_customers
    paid_without_conversion = paid_customers - converted_customers
    assert not converted_without_paid and not paid_without_conversion, (
        "Mismatch between converted customers and paid customers. "
        f"converted_without_paid={sorted(converted_without_paid)[:10]} "
        f"paid_without_conversion={sorted(paid_without_conversion)[:10]}"
    )

    aggregated = build_monthly_customer_activity(product_events, customers)
    compare_cols = [
        "customer_id",
        "activity_month",
        "nb_app_opens",
        "nb_transactions_imported",
        "nb_budgets_created",
        "nb_goals_created",
        "nb_dashboard_views",
        "is_active_month",
        "usage_segment",
    ]
    left = monthly_customer_activity[compare_cols].sort_values(["customer_id", "activity_month"]).reset_index(drop=True)
    right = aggregated[compare_cols].sort_values(["customer_id", "activity_month"]).reset_index(drop=True)
    assert left.equals(right), "monthly_customer_activity is not consistent with product_events."

    events_within_activation_window = product_events.merge(
        customers[["customer_id", "signup_date"]], on="customer_id", how="left"
    )
    events_within_activation_window["within_7_days"] = (
        events_within_activation_window["event_date"]
        <= events_within_activation_window["signup_date"] + pd.Timedelta(days=6)
    )
    windowed = events_within_activation_window[events_within_activation_window["within_7_days"]]
    activation_summary = (
        windowed.groupby("customer_id")["event_type"].agg(list).apply(
            lambda events: (
                any(event in {"bank_account_connected", "transaction_imported"} for event in events)
                and "budget_created" in events
            )
        )
    )
    activation_rate = activation_summary.reindex(customers["customer_id"], fill_value=False).mean()
    conversion_rate = subscriptions["converted_to_paid"].mean()
    annual_share = (
        subscriptions.loc[subscriptions["converted_to_paid"], "plan_type"].eq("annual").mean()
        if subscriptions["converted_to_paid"].any()
        else 0.0
    )

    monthly_subs = subscriptions[
        (subscriptions["converted_to_paid"]) & (subscriptions["plan_type"] == "monthly")
    ].copy()
    if monthly_subs.empty:
        monthly_churn = 0.0
    else:
        effective_end = monthly_subs["subscription_end_date"].fillna(DATASET_END)
        active_months = (
            effective_end.dt.to_period("M") - monthly_subs["subscription_start_date"].dt.to_period("M")
        ).apply(lambda period_delta: period_delta.n + 1)
        total_active_months = active_months.clip(lower=1).sum()
        monthly_cancellations = monthly_subs["subscription_status"].eq("cancelled").sum()
        monthly_churn = monthly_cancellations / total_active_months if total_active_months else 0.0

    assert TARGETS["activation_rate"][0] <= activation_rate <= TARGETS["activation_rate"][1], "Activation rate out of target range."
    assert TARGETS["conversion_rate"][0] <= conversion_rate <= TARGETS["conversion_rate"][1], (
        f"Conversion rate out of target range: {conversion_rate:.3f}"
    )
    assert TARGETS["annual_share"][0] <= annual_share <= TARGETS["annual_share"][1], "Annual share out of target range."
    assert TARGETS["monthly_churn"][0] <= monthly_churn <= TARGETS["monthly_churn"][1], (
        f"Monthly plan churn out of target range: {monthly_churn:.3f}"
    )


def export_csv(
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    payments: pd.DataFrame,
    product_events: pd.DataFrame,
    monthly_customer_activity: pd.DataFrame,
) -> None:
    output_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)

    customers.to_csv(output_dir / "customers.csv", index=False)
    subscriptions.to_csv(output_dir / "subscriptions.csv", index=False)
    payments.to_csv(output_dir / "payments.csv", index=False)
    product_events.to_csv(output_dir / "product_events.csv", index=False)
    monthly_customer_activity.to_csv(output_dir / "monthly_customer_activity.csv", index=False)


def main() -> None:
    rng = random.Random(RANDOM_SEED)

    customers = generate_customers(rng)
    subscriptions, customer_signals = generate_subscriptions(customers, rng)
    payments = generate_payments(subscriptions)
    product_events = generate_product_events(customers, subscriptions, customer_signals, rng)
    monthly_customer_activity = build_monthly_customer_activity(product_events, customers)

    for frame, date_columns in [
        (customers, ["signup_date"]),
        (subscriptions, ["trial_start_date", "trial_end_date", "subscription_start_date", "subscription_end_date"]),
        (payments, ["payment_date"]),
        (product_events, ["event_date"]),
        (monthly_customer_activity, ["activity_month"]),
    ]:
        for col in date_columns:
            if col in frame.columns:
                frame[col] = pd.to_datetime(frame[col], errors="coerce")

    run_quality_checks(customers, subscriptions, payments, product_events, monthly_customer_activity)
    export_csv(customers, subscriptions, payments, product_events, monthly_customer_activity)

    print("Dataset generated successfully in data/raw/")
    print(f"customers: {len(customers)} rows")
    print(f"subscriptions: {len(subscriptions)} rows")
    print(f"payments: {len(payments)} rows")
    print(f"product_events: {len(product_events)} rows")
    print(f"monthly_customer_activity: {len(monthly_customer_activity)} rows")


if __name__ == "__main__":
    main()
