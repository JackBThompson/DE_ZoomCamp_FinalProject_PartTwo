## Objective: Generate 5 fake CRM CSV files simulating messy legacy system exports.
## Intentional data quality issues: duplicate emails, mismatched IDs, inconsistent date formats, nulls, mixed case.

import pandas as pd
from faker import Faker
import random
import os

fake = Faker()
random.seed(42)
Faker.seed(42)

NUM_CUSTOMERS = 2000
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
os.makedirs(OUTPUT_DIR, exist_ok=True)

PLAN_TIERS = ['free', 'starter', 'growth', 'enterprise']
DEAL_STAGES = ['prospecting', 'qualified', 'proposal', 'negotiation', 'closed_won', 'closed_lost']
ACTIVITY_TYPES = ['email', 'call', 'demo', 'follow_up', 'support_ticket']
ACTIVITY_OUTCOMES = ['positive', 'neutral', 'negative', 'no_response']


# --- customers.csv ---
# Simulates a legacy CRM export
# Intentional issues: mixed case company names, duplicate emails, some nulls in phone

def generate_customers(n):
    rows = []
    emails_used = []

    for i in range(1, n + 1):
        email = fake.email()

        # Introduce ~5% duplicate emails
        if random.random() < 0.05 and emails_used:
            email = random.choice(emails_used)
        else:
            emails_used.append(email)

        # Mixed case company names
        company = fake.company()
        if random.random() < 0.3:
            company = company.lower()
        elif random.random() < 0.3:
            company = company.upper()

        # Two date formats — MM/DD/YYYY vs YYYY-MM-DD
        signup = fake.date_between(start_date='-3y', end_date='today')
        if random.random() < 0.4:
            signup_str = signup.strftime('%m/%d/%Y')
        else:
            signup_str = signup.strftime('%Y-%m-%d')

        rows.append({
            'customer_id': i,
            'name': fake.name(),
            'email': email,
            'company': company,
            'signup_date': signup_str,
            'plan_tier': random.choice(PLAN_TIERS),
            'country': fake.country_code(),
        })

    return pd.DataFrame(rows)


# --- contacts.csv ---
# Simulates a marketing platform export linked to customers
# Intentional issues: some customer_ids slightly off (mismatched), some nulls in role

def generate_contacts(customers_df):
    rows = []
    customer_ids = customers_df['customer_id'].tolist()

    for i in range(1, int(NUM_CUSTOMERS * 1.5) + 1):
        cid = random.choice(customer_ids)

        # ~8% mismatched IDs (referencing a customer that doesn't exist)
        if random.random() < 0.08:
            cid = cid + 10000

        rows.append({
            'contact_id': i,
            'customer_id': cid,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number() if random.random() > 0.15 else None,
            'role': fake.job() if random.random() > 0.1 else None,
        })

    return pd.DataFrame(rows)


# --- deals.csv ---
# Simulates a sales tool export
# Intentional issues: some nulls in close_date for open deals, mixed date formats

def generate_deals(customers_df):
    rows = []
    customer_ids = customers_df['customer_id'].tolist()

    for i in range(1, int(NUM_CUSTOMERS * 2) + 1):
        stage = random.choice(DEAL_STAGES)
        close_date = None

        if stage in ('closed_won', 'closed_lost'):
            d = fake.date_between(start_date='-2y', end_date='today')
            if random.random() < 0.4:
                close_date = d.strftime('%m/%d/%Y')
            else:
                close_date = d.strftime('%Y-%m-%d')

        rows.append({
            'deal_id': i,
            'customer_id': random.choice(customer_ids),
            'deal_value': round(random.uniform(500, 150000), 2),
            'deal_stage': stage,
            'close_date': close_date,
            'owner': fake.name(),
        })

    return pd.DataFrame(rows)


# --- activities.csv ---
# Simulates a support/CRM activity log
# Intentional issues: some timestamps malformed, mixed formats

def generate_activities(customers_df):
    rows = []
    customer_ids = customers_df['customer_id'].tolist()

    for i in range(1, int(NUM_CUSTOMERS * 3) + 1):
        ts = fake.date_time_between(start_date='-2y', end_date='now')

        # Mix timestamp formats
        if random.random() < 0.4:
            ts_str = ts.strftime('%m/%d/%Y %H:%M')
        else:
            ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')

        rows.append({
            'activity_id': i,
            'customer_id': random.choice(customer_ids),
            'activity_type': random.choice(ACTIVITY_TYPES),
            'timestamp': ts_str,
            'outcome': random.choice(ACTIVITY_OUTCOMES) if random.random() > 0.05 else None,
            'notes': fake.sentence() if random.random() > 0.3 else None,
        })

    return pd.DataFrame(rows)


# --- subscriptions.csv ---
# Simulates a billing system export
# Intentional issues: some MRR nulls for free tier, mixed date formats

def generate_subscriptions(customers_df):
    rows = []

    for _, customer in customers_df.iterrows():
        plan = customer['plan_tier']
        mrr = None

        if plan == 'starter':
            mrr = round(random.uniform(49, 99), 2)
        elif plan == 'growth':
            mrr = round(random.uniform(199, 499), 2)
        elif plan == 'enterprise':
            mrr = round(random.uniform(999, 9999), 2)

        renewal = fake.date_between(start_date='today', end_date='+2y')
        if random.random() < 0.4:
            renewal_str = renewal.strftime('%m/%d/%Y')
        else:
            renewal_str = renewal.strftime('%Y-%m-%d')

        rows.append({
            'subscription_id': int(customer['customer_id']),
            'customer_id': int(customer['customer_id']),
            'plan': plan,
            'mrr': mrr,
            'renewal_date': renewal_str,
            'status': random.choice(['active', 'active', 'active', 'churned', 'paused']),
        })

    return pd.DataFrame(rows)


if __name__ == '__main__':
    print(f"Generating {NUM_CUSTOMERS} customers...")

    customers_df     = generate_customers(NUM_CUSTOMERS)
    contacts_df      = generate_contacts(customers_df)
    deals_df         = generate_deals(customers_df)
    activities_df    = generate_activities(customers_df)
    subscriptions_df = generate_subscriptions(customers_df)

    customers_df.to_csv(f'{OUTPUT_DIR}/customers.csv', index=False)
    contacts_df.to_csv(f'{OUTPUT_DIR}/contacts.csv', index=False)
    deals_df.to_csv(f'{OUTPUT_DIR}/deals.csv', index=False)
    activities_df.to_csv(f'{OUTPUT_DIR}/activities.csv', index=False)
    subscriptions_df.to_csv(f'{OUTPUT_DIR}/subscriptions.csv', index=False)

    print(f"customers.csv     — {len(customers_df):,} rows")
    print(f"contacts.csv      — {len(contacts_df):,} rows")
    print(f"deals.csv         — {len(deals_df):,} rows")
    print(f"activities.csv    — {len(activities_df):,} rows")
    print(f"subscriptions.csv — {len(subscriptions_df):,} rows")
    print(f"\nFiles written to {OUTPUT_DIR}")
