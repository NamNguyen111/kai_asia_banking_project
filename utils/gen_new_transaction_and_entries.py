from connect_db_from_airflow import connect_to_db
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
fake = Faker()


def random_transaction_type():
    return random.choice(['DEBIT', 'TRANSFER', 'CREDIT'])

def get_one_or_two_accounts(num, cur):
    if num == 1:
        cur.execute("""
            SELECT account_id, balance, created_at FROM raw.accounts
            WHERE status = 'ACTIVE' AND account_id != 'BANK_CASH'
            ORDER BY RANDOM()
            LIMIT 1
        """)
        account = cur.fetchall()
        return account
    else:
        cur.execute("""
            SELECT account_id, balance, created_at FROM raw.accounts
            WHERE status = 'ACTIVE' AND account_type = 'CHECKING' AND account_id != 'BANK_CASH'
            ORDER BY RANDOM()
            LIMIT 2
        """)
        accounts = cur.fetchall()
        if len(accounts)<2:
            return 
        return accounts

def get_latest_transaction_time(cur, account_id):
    cur.execute("""
        SELECT MAX(created_at) FROM raw.transactions 
        WHERE from_account_id = %s OR to_account_id = %s
    """, (account_id, account_id))
    result = cur.fetchone()
    return result[0] if result[0] else None

def get_latest_transaction_time_for_both(cur, from_account_id, to_account_id):
    from_latest = get_latest_transaction_time(cur, from_account_id)
    to_latest = get_latest_transaction_time(cur, to_account_id)
    
    if from_latest and to_latest:
        return max(from_latest, to_latest)
    elif from_latest:
        return from_latest
    elif to_latest:
        return to_latest
    else:
        return None

# HÀM MỚI: Tạo thời gian giao dịch tự nhiên
def generate_natural_transaction_time(start_time, account_created_at):
    """
    Tạo thời gian giao dịch tự nhiên:
    - Nếu có giao dịch trước đó: tạo từ 1-72 giờ sau giao dịch cuối
    - Nếu chưa có giao dịch: tạo từ ngày tạo account đến hiện tại
    """
    now = datetime.now()
    
    if start_time:
        # Có giao dịch trước đó - tạo giao dịch mới từ 1-72 giờ sau
        min_gap = timedelta(hours=1)  # Tối thiểu 1 giờ
        max_gap = timedelta(hours=4)  # Tối đa 3 ngày
        
        earliest_time = start_time + min_gap
        latest_time = min(start_time + max_gap, now)
        
        if earliest_time >= latest_time:
            # Nếu không thể tạo thời gian hợp lệ, dùng thời gian hiện tại
            return now
        
        return fake.date_time_between(start_date=earliest_time, end_date=latest_time)
    else:
        # Chưa có giao dịch - tạo từ ngày tạo account
        if account_created_at >= now:
            return now
        return fake.date_time_between(start_date=account_created_at, end_date=now)

def get_money_from_bank_cash(cur):
    cur.execute("""
            SELECT balance FROM raw.accounts
            WHERE account_id = 'BANK_CASH'
        """)
    bank_cash_money = cur.fetchone()
    return bank_cash_money[0] if bank_cash_money else 0

def insert_new_transaction_entries(cur, transaction_data: dict):
    transaction_id = transaction_data['transaction_id']
    transfer_amount = transaction_data['amount']
    created_at = transaction_data['created_date_for_entries']
    transaction_type = transaction_data['transaction_type']
    
    match transaction_type:
        case 'TRANSFER':
            # entry sequence 1
            cur.execute(
                """
                INSERT INTO raw.transaction_entries(
                    entry_id, transaction_id, account_id, debit_amount, credit_amount,
                    balance_before, balance_after, entry_type, entry_sequence, description, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()), transaction_id, transaction_data['from_account_id'],
                    transfer_amount, 0, transaction_data['from_account_balance_before'],
                    transaction_data['from_account_balance_before'] - transfer_amount,
                    'DEBIT', 1, f'Transfer {transfer_amount} to {transaction_data["to_account_id"]}',
                    created_at    
                )
            )
            # entry sequence 2
            cur.execute(
                """
                INSERT INTO raw.transaction_entries(
                    entry_id, transaction_id, account_id, debit_amount, credit_amount,
                    balance_before, balance_after, entry_type, entry_sequence, description, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()), transaction_id, transaction_data['to_account_id'],
                    0, transfer_amount, transaction_data['to_account_balance_before'],
                    transaction_data['to_account_balance_before'] + transfer_amount,
                    'CREDIT', 2, f'Receive {transfer_amount} from {transaction_data["from_account_id"]}',
                    created_at    
                )
            )
            
        case 'CREDIT':
            money_in_bank_cash = get_money_from_bank_cash(cur=cur)
            # entry sequence 1
            cur.execute(
                """
                INSERT INTO raw.transaction_entries(
                    entry_id, transaction_id, account_id, debit_amount, credit_amount,
                    balance_before, balance_after, entry_type, entry_sequence, description, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()), transaction_id, "BANK_CASH",
                    transfer_amount, 0, money_in_bank_cash, money_in_bank_cash,
                    'DEBIT', 1, f'Transfer {transfer_amount} to {transaction_data["to_account_id"]}',
                    created_at    
                )
            )
            # entry sequence 2
            cur.execute(
                """
                INSERT INTO raw.transaction_entries(
                    entry_id, transaction_id, account_id, debit_amount, credit_amount,
                    balance_before, balance_after, entry_type, entry_sequence, description, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()), transaction_id, transaction_data['to_account_id'],
                    0, transfer_amount, transaction_data['to_account_balance_before'],
                    transaction_data['to_account_balance_before'] + transfer_amount,
                    'CREDIT', 2, f'Receive {transfer_amount} from BANK_CASH',
                    created_at    
                )
            )
            
        case 'DEBIT':
            money_in_bank_cash = get_money_from_bank_cash(cur=cur)
            # entry sequence 1
            cur.execute(
                """
                INSERT INTO raw.transaction_entries(
                    entry_id, transaction_id, account_id, debit_amount, credit_amount,
                    balance_before, balance_after, entry_type, entry_sequence, description, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()), transaction_id, transaction_data['from_account_id'],
                    transfer_amount, 0, transaction_data['from_account_balance_before'],
                    transaction_data['from_account_balance_before'] - transfer_amount,
                    'DEBIT', 1, f'Transfer {transfer_amount} to BANK_CASH',
                    created_at    
                )
            )
            # entry sequence 2
            cur.execute(
                """
                INSERT INTO raw.transaction_entries(
                    entry_id, transaction_id, account_id, debit_amount, credit_amount,
                    balance_before, balance_after, entry_type, entry_sequence, description, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()), transaction_id, "BANK_CASH",
                    0, transfer_amount, money_in_bank_cash, money_in_bank_cash,
                    'CREDIT', 2, f'Get {transfer_amount} in CASH from BANK_CASH',
                    created_at    
                )
            )

def insert_new_transaction_record(cur, transaction_data: dict):
    transaction_id = str(uuid.uuid4())
    transaction_data['transaction_id'] = transaction_id
    reference_number = fake.bothify(text='REF###############################################')
    description = fake.sentence(nb_words=6)
    
    if transaction_data['transaction_type'] == "TRANSFER":
        from_account_created_date = transaction_data['from_account_created_at']
        to_account_created_date = transaction_data['to_account_created_at']
        account_created_date = max(from_account_created_date, to_account_created_date)
        
        latest_transaction_time = get_latest_transaction_time_for_both(
            cur, transaction_data['from_account_id'], transaction_data['to_account_id']
        )
        
        # SỬA: Sử dụng hàm tạo thời gian tự nhiên
        created_at = generate_natural_transaction_time(latest_transaction_time, account_created_date)
        transaction_data['created_date_for_entries'] = created_at
        
        cur.execute("""
            INSERT INTO raw.transactions (
                transaction_id, reference_number, from_account_id, to_account_id, 
                amount, transaction_type, channel, description, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            transaction_id, reference_number, transaction_data['from_account_id'],
            transaction_data['to_account_id'], transaction_data['amount'], 
            transaction_data['transaction_type'], transaction_data['channel'],
            description, created_at
        ))
        insert_new_transaction_entries(cur=cur, transaction_data=transaction_data)
        
    elif transaction_data['transaction_type'] == "CREDIT":
        account_created_date = transaction_data['account_created_at']
        latest_transaction_time = get_latest_transaction_time(cur, transaction_data['to_account_id'])
        
        # SỬA: Sử dụng hàm tạo thời gian tự nhiên
        created_at = generate_natural_transaction_time(latest_transaction_time, account_created_date)
        transaction_data['created_date_for_entries'] = created_at
        
        cur.execute("""
            INSERT INTO raw.transactions (
                transaction_id, reference_number, from_account_id, to_account_id,
                amount, transaction_type, channel, description, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            transaction_id, reference_number, "BANK_CASH", transaction_data['to_account_id'],
            transaction_data['amount'], transaction_data['transaction_type'],
            transaction_data['channel'], description, created_at
        ))
        insert_new_transaction_entries(cur=cur, transaction_data=transaction_data)
        
    else:  # DEBIT
        account_created_date = transaction_data['account_created_at']
        latest_transaction_time = get_latest_transaction_time(cur, transaction_data['from_account_id'])
        
        # SỬA: Sử dụng hàm tạo thời gian tự nhiên
        created_at = generate_natural_transaction_time(latest_transaction_time, account_created_date)
        transaction_data['created_date_for_entries'] = created_at
        
        cur.execute("""
            INSERT INTO raw.transactions (
                transaction_id, reference_number, from_account_id, to_account_id,
                amount, transaction_type, channel, description, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            transaction_id, reference_number, transaction_data['from_account_id'], "BANK_CASH",
            transaction_data['amount'], "DEBIT", "BRANCH", description, created_at
        ))
        insert_new_transaction_entries(cur=cur, transaction_data=transaction_data)

def navigate_func(n=200):
    with connect_to_db() as conn:
        cur = conn.cursor()
        
        for i in range(n):
            transaction_type = random_transaction_type()
            
            match transaction_type:
                case 'DEBIT':
                    account = get_one_or_two_accounts(1, cur=cur)
                    if not account:
                        continue
                    from_account, from_balance, account_created_at = account[0]
                    amount = int(random.uniform(1_000, from_balance))
                    amount = round(amount, -5)
                    from_new_balance = from_balance - amount
                    
                    cur.execute("""
                        UPDATE raw.accounts SET balance = %s WHERE account_id = %s
                    """, (from_new_balance, from_account))
                    
                    transaction_data = {
                        'account_created_at': account_created_at,
                        'from_account_id': from_account,
                        'transaction_type': 'DEBIT',
                        'amount': amount,
                        'from_account_balance_before': from_balance,
                        'channel': 'BRANCH'
                    }
                    insert_new_transaction_record(cur=cur, transaction_data=transaction_data)

                case 'CREDIT':
                    account = get_one_or_two_accounts(1, cur=cur)
                    if not account:
                        continue
                    to_account, to_balance, account_created_at = account[0]
                    amount = int(random.uniform(1_000, 10_000_000))
                    amount = round(amount, -5)
                    to_new_balance = to_balance + amount
                    
                    cur.execute("""
                        UPDATE raw.accounts SET balance = %s WHERE account_id = %s
                    """, (to_new_balance, to_account))
                    
                    transaction_data = {
                        'account_created_at': account_created_at,
                        'to_account_id': to_account,
                        'transaction_type': 'CREDIT',
                        'amount': amount,
                        'to_account_balance_before': to_balance,
                        'channel': 'BRANCH'
                    }
                    insert_new_transaction_record(cur=cur, transaction_data=transaction_data)

                case 'TRANSFER':
                    accounts = get_one_or_two_accounts(2, cur=cur)
                    if not accounts or len(accounts) < 2:
                        continue
                    from_account, from_balance, from_account_created_at = accounts[0]
                    to_account, to_balance, to_account_created_at = accounts[1]
                    
                    upper_limit = min(from_balance, 100_000_000)
                    if upper_limit < 1_000:
                        amount = int(upper_limit)
                    else:
                        amount = int(random.uniform(1_000, upper_limit))
                    amount = round(amount, -5)
                    
                    from_new_balance = from_balance - amount
                    to_new_balance = to_balance + amount
                    
                    cur.execute("""
                        UPDATE raw.accounts SET balance = %s WHERE account_id = %s
                    """, (from_new_balance, from_account))
                    cur.execute("""
                        UPDATE raw.accounts SET balance = %s WHERE account_id = %s
                    """, (to_new_balance, to_account))
                    
                    transaction_data = {
                        'from_account_created_at': from_account_created_at,
                        'to_account_created_at': to_account_created_at,
                        'from_account_id': from_account,
                        'to_account_id': to_account,
                        'transaction_type': 'TRANSFER',
                        'amount': amount,
                        'from_account_balance_before': from_balance,
                        'to_account_balance_before': to_balance,
                        'channel': 'MOBILE'
                    }
                    insert_new_transaction_record(cur=cur, transaction_data=transaction_data)