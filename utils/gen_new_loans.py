# CREATE TABLE IF NOT EXISTS raw.loans (
#     loan_id SERIAL PRIMARY KEY,
#     customer_id VARCHAR(20) NOT NULL,
#     loan_type VARCHAR(50),                  -- Tín chấp, Thế chấp, Thấu chi...
#     loan_amount BIGINT NOT NULL,     -- Số tiền vay (theo hợp đồng)
#     interest_rate NUMERIC(5,2) NOT NULL,    -- %/năm
#     term_length INT NOT NULL,               -- Kỳ hạn (tháng)
#     start_date DATE NOT NULL,               -- Ngày giải ngân
#     maturity_date DATE NOT NULL,            -- Ngày đáo hạn
#     repayment_method VARCHAR(20),           -- EMI, INTEREST_ONLY, BULLET
#     penalty_rate NUMERIC(5,2),              -- % phạt trả chậm
#     collateral VARCHAR(255),                -- Tài sản thế chấp
#     status VARCHAR(50) DEFAULT 'ONGOING',   -- ONGOING, OVERDUE, CLOSED, DEFAULTED
#     created_at TIMESTAMP DEFAULT NOW(),
#     FOREIGN KEY (customer_id) REFERENCES raw.customers(customer_id)
# );

from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from faker import Faker
from connect_db_from_airflow import connect_to_db
from gen_new_account import get_customer_since_date
import random
from dateutil.relativedelta import relativedelta
import uuid
fake = Faker()
def get_a_random_customer_and_salary(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id, income_range FROM raw.customers WHERE customer_id != 'BANK00000001' ORDER BY RANDOM() LIMIT 1")
        result = cur.fetchone()
        return result[0],result[1] if result else None   # lấy id trực tiếp thay vì tuple


def insert_mock_loans(n=1, **kwargs):
    with connect_to_db() as conn:
        data = []
        for i in range(n):
            # Lấy 1 customer ngẫu nhiên và lương tháng (để tính vay tín chấp và thấu chi)
            customer_id, monthly_income = get_a_random_customer_and_salary(conn=conn)
            if not customer_id:
                continue
            # Unsecured Loan = Tín chấp, Mortgage = Thế chấp, Overdraft = Thấu chi
            loan_type = random.choice(["Unsecured Loan", "Mortgage", "Overdraft"])
            mortgage_ranges = [
                (100_000_000, 200_000_000),
                (200_000_000, 300_000_000),
                (300_000_000, 500_000_000),
                (500_000_000, 2_000_000_000),
                (2_000_000_000, 23_000_000_000)
            ]
            mortgage_weights = [0.8, 0.1, 0.095, 0.004, 0.001]
            collateral = None
            # Lựa số tiền vay
            if loan_type == "Unsecured Loan":
                loan_amount = monthly_income * int(random.uniform(1, 6))
                interest_rate = random.uniform(10.0, 20.0)
                term_length = random.choice([3,6,12,24])
                repayment_method = "EMI"
                # Phạt cao vì rủi ru thu hồi vốn cũng cao
                penalty_rate = random.uniform(36.0, 72.0)
            elif loan_type == "Overdraft":
                loan_amount = monthly_income * int(random.uniform(1, 6))
                interest_rate = random.uniform(15.0, 20.0)
                term_length = random.choice([3,6,12])
                repayment_method = "BULLET"
                penalty_rate = random.uniform(24.0, 60.0)
            else:
                low, high = random.choices(mortgage_ranges, weights=mortgage_weights, k=1)[0]
                tmp_loan = random.randint(low, high)
                loan_amount = round(tmp_loan, -5)
                term_length = random.choice([24,36,48,60,72,84,96,108])
                interest_rate = random.uniform(8.0, 12.0)
                repayment_method = random.choice(["INTEREST_ONLY", "EMI"])
                penalty_rate = random.uniform(12.0, 36.0)
                collateral = random.choice([
                    "House",          # Nhà
                    "Apartment",      # Căn hộ
                    "Land",           # Đất
                    "Car",            # Ô tô
                    "SavingsBook",    # Sổ tiết kiệm
                    "Gold",           # Vàng
                ])
            status = "ONGOING"
            customer_since = get_customer_since_date(conn=conn, customer_id=str(customer_id))
            customer_since = datetime.combine(customer_since[0], datetime.min.time())
            # end_date = hiện tại trừ 5 tháng
            end_date = datetime.now() - relativedelta(months=6)
            created_at = fake.date_time_between(start_date=customer_since, end_date=end_date)
            start_date = created_at
            maturity_date = start_date + relativedelta(months=term_length)
            data.append((
                    customer_id,
                    loan_type,
                    loan_amount,
                    interest_rate,
                    term_length,
                    start_date,
                    maturity_date,
                    repayment_method,
                    penalty_rate,
                    collateral,
                    status,
                    created_at
            ))
        sql = """
            INSERT INTO raw.loans (
                customer_id,
                    loan_type,
                    loan_amount,
                    interest_rate,
                    term_length,
                    start_date,
                    maturity_date,
                    repayment_method,
                    penalty_rate,
                    collateral,
                    status,
                    created_at
            )
            VALUES %s
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, data)
