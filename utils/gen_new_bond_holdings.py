from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from faker import Faker
from connect_db_from_airflow import connect_to_db
from gen_new_account import get_customer_since_date
import random
from dateutil.relativedelta import relativedelta
import uuid

fake = Faker()

bond_list = ["GOVB001",
"GOVB002",
"CORP001",
"CORP002",
"ZERO001",
"GOVB003",
"CORP003",
"BANK001",
"ZERO002"
]

def get_a_random_customer(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM raw.customers WHERE customer_id != 'BANK00000001' ORDER BY RANDOM() LIMIT 1")
        result = cur.fetchone()
        return result[0] if result else None   # lấy id trực tiếp thay vì tuple


def insert_mock_bond_holdings(n=1, **kwargs):
    with connect_to_db() as conn:
        data = []
        for i in range(n):
            # Lấy 1 customer ngẫu nhiên
            customer_id = get_a_random_customer(conn=conn)
            if not customer_id:
                continue
            bond_transaction_id = str(uuid.uuid4())
            bond_code = random.choice(bond_list)
            # Random số lượng trái phiếu mua
            buy_quantity = random.uniform(10, 100)
            status = "ACTIVE"
            customer_since = get_customer_since_date(conn=conn, customer_id=str(customer_id))
            created_at = fake.date_time_between(start_date=customer_since, end_date='now')

            data.append((
                bond_transaction_id, bond_code, customer_id, buy_quantity, status, created_at
            ))

        sql = """
            INSERT INTO raw.bond_holdings (
                bond_transaction_id, bond_code, customer_id, buy_quantity, status, created_at
            )
            VALUES %s
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, data)
