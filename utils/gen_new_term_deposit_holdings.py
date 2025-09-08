from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from faker import Faker
from connect_db_from_airflow import connect_to_db
from gen_new_account import get_customer_since_date
import random
from dateutil.relativedelta import relativedelta

fake = Faker()

def get_a_random_customer(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM raw.customers WHERE customer_id != 'BANK00000001' ORDER BY RANDOM() LIMIT 1")
        result = cur.fetchone()
        return result[0] if result else None   # lấy id trực tiếp thay vì tuple

def insert_mock_term_deposit_holdings(n=1, **kwargs):
    with connect_to_db() as conn:
        data = []
        checking_ranges = [
            (0, 30_000_000),
            (30_000_000, 100_000_000),
            (100_000_000, 1_000_000_000),
            (1_000_000_000, 25_000_000_000)
        ]
        weights = [0.8, 0.1, 0.095, 0.005]

        for i in range(n):
            # Lấy 1 customer ngẫu nhiên
            customer_id = get_a_random_customer(conn=conn)
            if not customer_id:
                continue

            # Random số tiền gửi
            low, high = random.choices(checking_ranges, weights=weights, k=1)[0]
            principal = random.randint(low, high)
            principal = round(principal, -5)

            # Random lãi suất
            interest_rate = round(random.uniform(5.0, 10.0), 2)
            interest_payment_interval = random.choice([3, 6, 12])

            customer_since = get_customer_since_date(customer_id=str(customer_id), conn=conn)   
            start_date = fake.date_time_between(start_date=customer_since, end_date='now')
            years_to_add = random.choice([1, 2, 3])  # Random số năm đáo hạn
            maturity_date = start_date + relativedelta(years=years_to_add)
            interest_calculation_method = random.choice(["SIMPLE", "COMPOUND"])
            status = "ACTIVE"
            created_at = start_date
            data.append((
                customer_id, principal, interest_rate, interest_payment_interval,
                start_date, maturity_date, interest_calculation_method, status, created_at
            ))

        sql = """
            INSERT INTO raw.term_deposit_holdings (
                customer_id, principal, interest_rate, interest_payment_interval, start_date, maturity_date, interest_calculation_method,
                status, created_at
            )
            VALUES %s
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, data)
