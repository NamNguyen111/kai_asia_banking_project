from psycopg2.extras import execute_values
from faker import Faker
from datetime import datetime, timedelta
import random
from connect_db_from_airflow import connect_to_db
fake = Faker()

def get_next_account_id(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT account_id FROM raw.accounts WHERE account_id <> 'BANK_CASH' ORDER BY account_id DESC LIMIT 1")
        last_id = cur.fetchone()
        if last_id:
            last_numeric = int(last_id[0][3:])  # Bỏ ACC
            return last_numeric + 1
        else:
            return 1

def get_existing_customer_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM raw.customers")
        return [row[0] for row in cur.fetchall()]

def get_existing_branch_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT branch_id FROM raw.branches")
        return [row[0] for row in cur.fetchall()]

def insert_mock_accounts(n=1, **kwargs):
    with connect_to_db() as conn:
        next_id = get_next_account_id(conn)
        customer_ids = get_existing_customer_ids(conn)
        branch_ids = get_existing_branch_ids(conn)

        if not customer_ids:
            raise Exception("No customers found in raw.customers")
        if not branch_ids:
            raise Exception("No branches found in raw.branches")

        data = []
        for i in range(n):
            numeric_id = next_id + i
            account_id = f"ACC{numeric_id:017d}"

            customer_id = random.choice(customer_ids)
            branch_id = random.choice(branch_ids)
            account_number = fake.numerify('15##########')  # e.g., 15-digit number

            account_type = random.choice(['SAVINGS', 'CHECKING'])
            status = 'ACTIVE'
            balance = round(random.uniform(10, 500_000_000), 2)

            # created_at = fake.date_time_between(start_date='-1y', end_date='now')
            created_at = datetime.now()
            updated_at = created_at

            data.append((
                account_id, customer_id, account_number, account_type,
                balance, status, branch_id, created_at, updated_at
            ))

        sql = """
            INSERT INTO raw.accounts (
                account_id, customer_id, account_number, account_type,
                balance, status, branch_id, created_at, updated_at
            )
            VALUES %s
            ON CONFLICT (account_id) DO NOTHING
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, data)
