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

def insert_mock_fund_cert_holdings(n=1, **kwargs):
    with connect_to_db() as conn:
        data = []
        for i in range(n):
            fund_cert_holding_id = str(uuid.uuid4())
            # Lấy 1 customer ngẫu nhiên
            customer_id = get_a_random_customer(conn=conn)
            if not customer_id:
                continue
            # Random số lượng chứng chỉ quỹ
            units = random.uniform(1000, 10000)
            nav_price = round(random.uniform(10000, 14000),-3)
            # Tính start_date, sẽ trong khoảng customer_since tới cách đây 6 tháng
            customer_since = get_customer_since_date(customer_id=str(customer_id), conn=conn)   
            customer_since = datetime.combine(customer_since[0], datetime.min.time()) 
            tmp_date = datetime.now() - relativedelta(months=1)
            purchase_date = fake.date_time_between(start_date=customer_since, end_date=tmp_date)
            status = "ACTIVE"
            created_at = purchase_date
            data.append((
                fund_cert_holding_id, customer_id, units, nav_price, purchase_date,
                status, created_at
            ))

        sql = """
            INSERT INTO raw.fund_certificate_holdings (
                fund_cert_holding_id, customer_id, units, nav_price, purchase_date,
                status, created_at
            )
            VALUES %s
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, data)
