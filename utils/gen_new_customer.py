from psycopg2.extras import execute_values
from faker import Faker
from datetime import datetime
from connect_db_from_airflow import connect_to_db

fake = Faker()

def get_next_customer_id(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM raw.customers ORDER BY customer_id DESC LIMIT 1")
        result = cur.fetchone()
        if result:
            last_num = int(result[0][3:])
        else:
            last_num = 0
        return last_num + 1

def insert_mock_customers(n=1, **kwargs):
    with connect_to_db() as conn:
        next_id = get_next_customer_id(conn)
        data = []
        for i in range(n):
            numeric_id = next_id + i
            customer_id = f"CUS{numeric_id:017d}"  # zero-padded
            full_name = fake.name()
            phone = '0' + ''.join(fake.random_choices(elements='0123456789', length=9))
            email = fake.user_name().lower() + '@gmail.com'
            id_number = fake.bothify(text='############')
            address = fake.address().replace('\n', ', ')
            date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=60)
            status = 'ACTIVE'
            # created_at = fake.date_time_between(start_date='-1y', end_date='now')
            created_at = datetime.now()
            updated_at = created_at
            data.append((customer_id, full_name, phone, email, id_number, address, date_of_birth, status, created_at, updated_at))
        
        sql = """
            INSERT INTO raw.customers (
                customer_id, full_name, phone, email, id_number, address,
                date_of_birth, status, created_at, updated_at
            )
            VALUES %s
            ON CONFLICT (customer_id) DO NOTHING
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, data)
