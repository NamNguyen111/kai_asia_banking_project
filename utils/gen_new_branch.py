from psycopg2.extras import execute_values
from faker import Faker
from datetime import datetime
from connect_db_from_airflow import connect_to_db
fake = Faker()

def get_next_branch_id(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT branch_id FROM raw.branches ORDER BY branch_id DESC LIMIT 1")
        result = cur.fetchone()
        if result:
            last_id = result[0]  # e.g., BR00000235
            last_num = int(last_id[2:])  # remove 'BR' prefix
        else:
            last_num = 0
        return last_num + 1  # next numeric part


def insert_mock_branches(n=1):
    with connect_to_db() as conn:
        next_id = get_next_branch_id(conn)
        data = []
        for i in range(n):
            numeric_id = next_id + i
            branch_id = f"BR{numeric_id:08d}"  # e.g., BR00000001
            branch_name = fake.company()
            address = fake.address().replace('\n', ', ')
            status = 'ACTIVE'
            # created_at = fake.date_time_between(start_date='-1y', end_date='now')
            created_at = datetime.now()
            data.append((branch_id, branch_name, address, status, created_at))
        sql = """
            INSERT INTO raw.branches (branch_id, branch_name, address, status, created_at)
            VALUES %s
            ON CONFLICT (branch_id) DO NOTHING
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, data)

