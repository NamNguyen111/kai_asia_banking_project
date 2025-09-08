from datetime import datetime, timedelta, date, time
from psycopg2.extras import execute_values
from faker import Faker
from connect_db_from_airflow import connect_to_db
from gen_new_account import get_customer_since_date
import random
from dateutil.relativedelta import relativedelta
from datetime import date
import uuid
fake = Faker()
def get_a_random_customer_and_salary(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id, income_range FROM raw.customers WHERE customer_id != 'BANK00000001' ORDER BY RANDOM() LIMIT 1")
        result = cur.fetchone()
        return result[0],result[1] if result else None   # lấy id trực tiếp thay vì tuple

def EMI_calculator(loan_amount, interest_rate, term_length):
    """
    loan_amount: số tiền vay (VND)
    interest_rate: lãi suất năm (%)
    term_length: số tháng vay
    return: số tiền trả mỗi tháng (gần như bằng nhau)
    """
    r = (interest_rate / 100) / 12  # lãi suất theo tháng
    emi_numerator = loan_amount * r
    emi_denominator = 1 - pow((1 + r), -term_length)
    emi = emi_numerator / emi_denominator
    return emi

def InterestOnly_calculator(loan_amount, interest_rate, term_length):
    """
    Chỉ trả lãi hàng tháng, gốc trả cuối kỳ
    """
    r = (interest_rate / 100) / 12
    monthly_interest = loan_amount * r
    return monthly_interest

def insert_mock_loans(n=1, **kwargs):
    with connect_to_db() as conn:
        with conn.cursor() as cur:
            for i in range(n):
                loan_id = uuid.uuid4()
                # Lấy 1 customer ngẫu nhiên và lương tháng
                customer_id, monthly_income = get_a_random_customer_and_salary(conn=conn)
                if not customer_id:
                    continue

                # Loan type
                loan_type = random.choice(["Unsecured Loan", "Mortgage"])
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
                    remaining_balance = loan_amount
                    interest_rate = random.uniform(10.0, 20.0)
                    term_length = random.choice([3,6,12,24])
                    repayment_method = "EMI"
                    penalty_rate = random.uniform(36.0, 72.0)
                else:  # Mortgage
                    low, high = random.choices(mortgage_ranges, weights=mortgage_weights, k=1)[0]
                    tmp_loan = random.randint(low, high)
                    loan_amount = round(tmp_loan, -5)
                    remaining_balance = loan_amount
                    term_length = random.choice([24,36,48,60,72,84,96,108])
                    interest_rate = random.uniform(8.0, 12.0)
                    repayment_method = random.choice(["INTEREST_ONLY", "EMI"])
                    penalty_rate = random.uniform(12.0, 36.0)
                    collateral = random.choice([
                        "House", "Apartment", "Land", "Car", "SavingsBook", "Gold"
                    ])

                status = "ONGOING"
                customer_since = get_customer_since_date(conn=conn, customer_id=str(customer_id))
                created_at = fake.date_time_between(start_date=customer_since, end_date='now')
                start_date = created_at
                maturity_date = start_date + relativedelta(months=term_length)

                cur.execute("""
                    INSERT INTO raw.loans (
                        loan_id,
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
                        remaining_balance,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(loan_id),
                    str(customer_id),
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
                    remaining_balance,
                    created_at
                ))

                insert_mock_loan_repayments(
                    cur=cur,
                    loan_id=loan_id,
                    loan_amount=loan_amount,
                    interest_rate=interest_rate,
                    term_length=term_length,
                    start_date=start_date,
                    repayment_method=repayment_method,
                    penalty_rate=penalty_rate,
                    maturity_date=maturity_date
                )

        conn.commit()


# def insert_mock_loans(n=1, **kwargs):
#     with connect_to_db() as conn:
#         data = []
#         for i in range(n):
#             loan_id = uuid.uuid4()
#             # Lấy 1 customer ngẫu nhiên và lương tháng (để tính vay tín chấp và thấu chi)
#             customer_id, monthly_income = get_a_random_customer_and_salary(conn=conn)
#             if not customer_id:
#                 continue
#             # Unsecured Loan = Tín chấp, Mortgage = Thế chấp, Overdraft = Thấu chi
#             loan_type = random.choice(["Unsecured Loan", "Mortgage"])
#             mortgage_ranges = [
#                 (100_000_000, 200_000_000),
#                 (200_000_000, 300_000_000),
#                 (300_000_000, 500_000_000),
#                 (500_000_000, 2_000_000_000),
#                 (2_000_000_000, 23_000_000_000)
#             ]
#             mortgage_weights = [0.8, 0.1, 0.095, 0.004, 0.001]
#             collateral = None
#             # Lựa số tiền vay
#             if loan_type == "Unsecured Loan":
#                 loan_amount = monthly_income * int(random.uniform(1, 6))
#                 remaining_balance = loan_amount
#                 interest_rate = random.uniform(10.0, 20.0)
#                 term_length = random.choice([3,6,12,24])
#                 repayment_method = "EMI"
#                 # Phạt cao vì rủi ru thu hồi vốn cũng cao
#                 penalty_rate = random.uniform(36.0, 72.0)
#             # elif loan_type == "Overdraft":
#             #     loan_amount = monthly_income * int(random.uniform(1, 6))
#             #     interest_rate = random.uniform(15.0, 20.0)
#             #     term_length = random.choice([3,6,12])
#             #     repayment_method = "BULLET"
#             #     penalty_rate = random.uniform(24.0, 60.0)
#             else:
#                 low, high = random.choices(mortgage_ranges, weights=mortgage_weights, k=1)[0]
#                 tmp_loan = random.randint(low, high)
#                 loan_amount = round(tmp_loan, -5)
#                 remaining_balance = loan_amount
#                 term_length = random.choice([24,36,48,60,72,84,96,108])
#                 interest_rate = random.uniform(8.0, 12.0)
#                 repayment_method = random.choice(["INTEREST_ONLY", "EMI"])
#                 penalty_rate = random.uniform(12.0, 36.0)
#                 collateral = random.choice([
#                     "House",          # Nhà
#                     "Apartment",      # Căn hộ
#                     "Land",           # Đất
#                     "Car",            # Ô tô
#                     "SavingsBook",    # Sổ tiết kiệm
#                     "Gold",           # Vàng
#                 ])
#             status = "ONGOING"
#             customer_since = get_customer_since_date(conn=conn, customer_id=str(customer_id))
#             created_at = fake.date_time_between(start_date=customer_since, end_date='now')
#             start_date = created_at
#             maturity_date = start_date + relativedelta(months=term_length)
#             data.append((
#                     loan_id,
#                     customer_id,
#                     loan_type,
#                     loan_amount,
#                     interest_rate,
#                     term_length,
#                     start_date,
#                     maturity_date,
#                     repayment_method,
#                     penalty_rate,
#                     collateral,
#                     status,
#                     remaining_balance,
#                     created_at
#             ))
#             insert_mock_loan_repayments(cur=conn.cursor(), 
#                                         loan_id=loan_id,
#                                          loan_amount = loan_amount, 
#                                          interest_rate = interest_rate,
#                                            term_length = term_length,
#                                              start_date = start_date,
#                                                repayment_method = repayment_method,
#                                                  penalty_rate = penalty_rate)

#         sql = """
#             INSERT INTO raw.loans (
#                     loan_id,
#                     customer_id,
#                     loan_type,
#                     loan_amount,
#                     interest_rate,
#                     term_length,
#                     start_date,
#                     maturity_date,
#                     repayment_method,
#                     penalty_rate,
#                     collateral,
#                     status,
#                     remaining_balance,
#                     created_at
#             )
#             VALUES %s
#         """
#         with conn.cursor() as cur:
#             execute_values(cur, sql, data)


def random_workhour_datetime(d: date) -> datetime:
    hour = random.randint(9, 16)  # từ 9h đến 16h
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime.combine(d, time(hour, minute, second))



def insert_mock_loan_repayments(cur, loan_id, loan_amount, interest_rate, term_length, start_date, repayment_method, penalty_rate, maturity_date):
    tmp_date = start_date
    today = datetime.today()
    if repayment_method == "EMI":
        """
        EMI là kiểu loan mỗi kì hạn sẽ trả 1 phần nợ gốc và 1 phần lãi bằng nhau
        """
        emi = EMI_calculator(loan_amount=loan_amount, interest_rate=interest_rate, term_length=term_length)
        tmp_loan_amount = loan_amount
        r = (interest_rate / 100) / 12  # lãi suất theo tháng
        while tmp_date + relativedelta(months=1) <= min(today, maturity_date):
            # Tính due_date cho record hiện tại
            due_date = tmp_date + relativedelta(months=1)
            # Tính phần lãi phải trả trong kì này 
                # interest_paid_t = outstanding_principal_prev * r (lãi kỳ này = dư nợ gốc * lãi suất tháng)
            interest_paid_t = tmp_loan_amount * r
            # Tính phần gốc phải trả trong kì này
            principal_paid_t = emi - interest_paid_t
            # Tính dư nợ sau khi trả kỳ này
            outstanding_principal_t = tmp_loan_amount - principal_paid_t
            tmp_loan_amount = outstanding_principal_t
            # Random xem trả nợ đúng hạn hay chậm để phạt, 85% trả đúng hạn
            if random.random() < 0.85:
                cur.execute(
                    """
                    INSERT INTO raw.loan_repayments(
                        loan_id,
                        due_date,
                        repayment_date,
                        principal_paid,
                        interest_paid,
                        late_fee_paid,
                        remaining_balance,
                        status,
                        payment_method,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        loan_id,
                        due_date,
                        due_date, # Vì trả đúng hạn nên ngày hẹn và ngày thực trả là cùng 1 ngày
                        principal_paid_t,
                        interest_paid_t,
                        0, # Trả đúng hạn nên k phạt
                        outstanding_principal_t,
                        "ON_TIME",
                        random.choice(["CASH", "TRANSFER"]),
                        random_workhour_datetime(due_date)    
                    )
                )
            else:
                late_day = random.choice([1, 2])
                penalty_number = emi * (penalty_rate/100) * (late_day / 365)
                cur.execute(
                    """
                    INSERT INTO raw.loan_repayments(
                        loan_id,
                        due_date,
                        repayment_date,
                        principal_paid,
                        interest_paid,
                        late_fee_paid,
                        remaining_balance,
                        status,
                        payment_method,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        loan_id,
                        due_date,
                        due_date + timedelta(days=late_day), # Random trả chậm 1 hoặc 2 ngày
                        principal_paid_t,
                        interest_paid_t,
                        penalty_number, # Phạt trả chậm
                        outstanding_principal_t,
                        "LATE",
                        random.choice(["CASH", "TRANSFER"]),
                        random_workhour_datetime(due_date)    
                    )
                )
            # Cập nhật số dư trong raw.loans
            cur.execute("""
                        UPDATE raw.loans
                        SET remaining_balance = %s
                        WHERE loan_id = %s
                    """, (outstanding_principal_t, loan_id))
            tmp_date = tmp_date + relativedelta(months=1)
            print(tmp_loan_amount)
        if tmp_loan_amount < 1:
            cur.execute("""
                        UPDATE raw.loans
                        SET status = %s
                        WHERE loan_id = %s
                    """, ("CLOSED", loan_id))
        # print("/////////////////////////")
    elif repayment_method == "INTEREST_ONLY":
        """
        Interest_Only là chỉ trả 1 phần lãi, gốc trả cuối kì + 1 phần lãi cuối
        """
        monthly_interest_pay_amount = InterestOnly_calculator(loan_amount=loan_amount, interest_rate=interest_rate, term_length=term_length)
        tmp_loan_amount = loan_amount
        r = (interest_rate / 100) / 12  # lãi suất theo tháng
        while tmp_date + relativedelta(months=1) < today:
            # Tính due_date cho record hiện tại
            due_date = tmp_date + relativedelta(months=1)
            # Random xem trả nợ đúng hạn hay chậm để phạt, 85% trả đúng hạn
            if random.random() < 0.85:
                cur.execute(
                    """
                    INSERT INTO raw.loan_repayments(
                        loan_id,
                        due_date,
                        repayment_date,
                        principal_paid,
                        interest_paid,
                        late_fee_paid,
                        remaining_balance,
                        status,
                        payment_method,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        loan_id,
                        due_date,
                        due_date, # Vì trả đúng hạn nên ngày hẹn và ngày thực trả là cùng 1 ngày
                        0,  # Không trả gốc
                        monthly_interest_pay_amount,
                        0, # Trả đúng hạn nên k phạt
                        loan_amount,
                        "ON_TIME",
                        random.choice(["CASH", "TRANSFER"]),
                        random_workhour_datetime(due_date)    
                    )
                )
            else:
                late_day = random.choice([1, 2])
                penalty_number = monthly_interest_pay_amount * (penalty_rate/100) * (late_day / 365)
                cur.execute(
                    """
                    INSERT INTO raw.loan_repayments(
                        loan_id,
                        due_date,
                        repayment_date,
                        principal_paid,
                        interest_paid,
                        late_fee_paid,
                        remaining_balance,
                        status,
                        payment_method,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        loan_id,
                        due_date,
                        due_date + timedelta(days=late_day), # Random trả chậm 1 hoặc 2 ngày
                        0,
                        monthly_interest_pay_amount,
                        penalty_number, # Phạt trả chậm
                        loan_amount,
                        "LATE",
                        random.choice(["CASH", "TRANSFER"]),
                        random_workhour_datetime(due_date)    
                    )
                )
            tmp_date = tmp_date + relativedelta(months=1)
        # print("/////////////////////////")
        if tmp_loan_amount < 1:
            cur.execute("""
                        UPDATE raw.loans
                        SET status = %s
                        WHERE loan_id = %s
                    """, ("CLOSED", loan_id))
    