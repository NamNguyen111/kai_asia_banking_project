from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from faker import Faker
from connect_db_from_airflow import connect_to_db
from gen_new_account import get_customer_since_date
import random
from dateutil.relativedelta import relativedelta
import uuid
fake = Faker()

def get_a_random_customer(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM raw.customers WHERE customer_id != 'BANK00000001' ORDER BY RANDOM() LIMIT 1")
        result = cur.fetchone()
        return result[0] if result else None   # lấy id trực tiếp thay vì tuple

def insert_mock_cert_of_deposit_holdings(n=1, **kwargs):
    with connect_to_db() as conn:
        data = []
        for i in range(n):
            customer_id = get_a_random_customer(conn=conn)
            # Lựa giá trị của chứng chỉ cho hợp lý:
            amount_ranges = [
                (0, 50_000_000),
                (50_000_000, 100_000_000),
                (100_000_000, 500_000_000),
                (500_000_000, 2_000_000_000),
                (2_000_000_000, 10_000_000_000)
            ]
            weights = [0.8, 0.1, 0.095, 0.004, 0.001]
            low, high = random.choices(amount_ranges, weights=weights, k=1)[0]
            tmp_amount = random.randint(low, high)
            amount = round(tmp_amount, -5)
            # Interest rate
            interest_rate = random.uniform(6.0, 8.0)
            # Tính start_date, sẽ trong khoảng customer_since tới cách đây 6 tháng
            customer_since = get_customer_since_date(customer_id=str(customer_id), conn=conn)   
            start_date = fake.date_time_between(start_date=customer_since, end_date='now')
            years_to_add = random.choice([1, 2, 3, 4, 5])  # Random số năm đáo hạn
            maturity_date = start_date + relativedelta(years=years_to_add)
            interest_calculation_method = random.choice(["SIMPLE", "COMPOUND"])
            term_length = years_to_add * 12
            status = "ACTIVE"
            created_at = start_date
            data.append((
                customer_id, amount, interest_rate, start_date, maturity_date, term_length, interest_calculation_method,
                status, created_at
            ))

        sql = """
            INSERT INTO raw.certificate_of_deposit_holdings (
                customer_id, amount, interest_rate, start_date, maturity_date, term_length, interest_calculation_method,
                status, created_at
            )
            VALUES %s
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, data)
