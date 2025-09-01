from connect_db_from_airflow import connect_to_db
import uuid
import random
from datetime import datetime
from faker import Faker
import psycopg2
fake = Faker()

"""
CODE FILE NÀY RẤT BẨN MẮT
"""

def random_transaction_type():
    # debit = rút tiền, credit = nạp tiền
    return random.choice(['DEBIT', 'TRANSFER', 'CREDIT'])

# Hàm này trả về tuple account tùy trường hợp
def get_one_or_two_accounts(num, cur):
    if num == 1:
        # Num == 1: Case là debit hoặc credit -> Chỉ lấy 1 account ra để thao tác, không gọi đến hàm gen_entries
        cur.execute("""
            SELECT account_id, balance FROM raw.accounts
            WHERE status = 'ACTIVE'
            ORDER BY RANDOM()
            LIMIT 1
        """)
        account = cur.fetchall()
        return account
    else:
        # Num == 2: Case là transfer -> lấy ra 2 accounts để thao tác, gọi đến gen_entries để lưu bút toán kép
        cur.execute("""
            SELECT account_id, balance FROM raw.accounts
            WHERE status = 'ACTIVE' AND account_type = 'CHECKING'
            ORDER BY RANDOM()
            LIMIT 2
        """)
        accounts = cur.fetchall()
        if len(accounts)<2:
            return 
        return accounts
    
def get_money_from_bank_cash(cur):
    cur.execute("""
            SELECT balance FROM raw.accounts
            WHERE account_id = 'BANK_CASH'
        """)
    bank_cash_money = cur.fetchone()  # Chỉ cần 1 record
    return bank_cash_money[0] if bank_cash_money else 0


def insert_new_transaction_entries(cur, transaction_data: dict):
    transaction_id = transaction_data['transaction_id']
    transfer_amount = transaction_data['amount']
    created_at = datetime.now()
    transaction_type = transaction_data['transaction_type']
    match transaction_type:
        case 'TRANSFER':
            # entry sequence 1
            cur.execute(
                """
                INSERT INTO raw.transaction_entries(
                    entry_id,
                    transaction_id,
                    account_id,
                    debit_amount,
                    credit_amount,
                    balance_before,
                    balance_after,
                    entry_type,
                    entry_sequence,
                    description,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()),
                    transaction_id,
                    transaction_data['from_account_id'],
                    transfer_amount,
                    0,
                    transaction_data['from_account_balance_before'],
                    transaction_data['from_account_balance_before'] - transfer_amount,
                    'DEBIT',
                    1,
                    f'Transfer {transfer_amount} to {transaction_data["to_account_id"]}',
                    created_at    
                )
            )
            # entry sequence 2
            cur.execute(
                """
                INSERT INTO raw.transaction_entries(
                    entry_id,
                    transaction_id,
                    account_id,
                    debit_amount,
                    credit_amount,
                    balance_before,
                    balance_after,
                    entry_type,
                    entry_sequence,
                    description,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()),
                    transaction_id,
                    transaction_data['to_account_id'],
                    0,
                    transfer_amount,
                    transaction_data['to_account_balance_before'],
                    transaction_data['to_account_balance_before'] + transfer_amount,
                    'CREDIT',
                    2,
                    f'Receive {transfer_amount} from {transaction_data["from_account_id"]}',
                    created_at    
                )
            )
        case 'CREDIT':
            # entry sequence 1
            money_in_bank_cash = get_money_from_bank_cash(cur=cur)
            cur.execute(
                """
                INSERT INTO raw.transaction_entries(
                    entry_id,
                    transaction_id,
                    account_id,
                    debit_amount,
                    credit_amount,
                    balance_before,
                    balance_after,
                    entry_type,
                    entry_sequence,
                    description,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()),
                    transaction_id,
                    "BANK_CASH",
                    transfer_amount,
                    0,
                    money_in_bank_cash,
                    money_in_bank_cash,
                    'DEBIT',
                    1,
                    f'Transfer {transfer_amount} to {transaction_data["to_account_id"]}',
                    created_at    
                )
            )
            # entry sequence 2
            cur.execute(
                """
                INSERT INTO raw.transaction_entries(
                    entry_id,
                    transaction_id,
                    account_id,
                    debit_amount,
                    credit_amount,
                    balance_before,
                    balance_after,
                    entry_type,
                    entry_sequence,
                    description,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()),
                    transaction_id,
                    transaction_data['to_account_id'],
                    0,
                    transfer_amount,
                    transaction_data['to_account_balance_before'],
                    transaction_data['to_account_balance_before'] + transfer_amount,
                    'CREDIT',
                    2,
                    f'Receive {transfer_amount} from BANK_CASH',
                    created_at    
                )
            )
        case 'DEBIT':
            # entry sequence 1
            money_in_bank_cash = get_money_from_bank_cash(cur=cur)
            cur.execute(
                """
                INSERT INTO raw.transaction_entries(
                    entry_id,
                    transaction_id,
                    account_id,
                    debit_amount,
                    credit_amount,
                    balance_before,
                    balance_after,
                    entry_type,
                    entry_sequence,
                    description,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()),
                    transaction_id,
                    transaction_data['from_account_id'],
                    transfer_amount,
                    0,
                    transaction_data['from_account_balance_before'],
                    transaction_data['from_account_balance_before'] - transfer_amount,
                    'DEBIT',
                    1,
                    f'Transfer {transfer_amount} to BANK_CASH',
                    created_at    
                )
            )
            # entry sequence 2
            cur.execute(
                """
                INSERT INTO raw.transaction_entries(
                    entry_id,
                    transaction_id,
                    account_id,
                    debit_amount,
                    credit_amount,
                    balance_before,
                    balance_after,
                    entry_type,
                    entry_sequence,
                    description,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(uuid.uuid4()),
                    transaction_id,
                    "BANK_CASH",
                    0,
                    transfer_amount,
                    money_in_bank_cash,
                    money_in_bank_cash,
                    'CREDIT',
                    2,
                    f'Get {transfer_amount} in CASH from BANK_CASH',
                    created_at    
                )
            )
    

def insert_new_transaction_record(cur, transaction_data: dict):
    transaction_id = str(uuid.uuid4())
    transaction_data['transaction_id'] = transaction_id
    reference_number = fake.bothify(text='REF###############################################')
    description = fake.sentence(nb_words=6)
    created_at = datetime.now()
    if transaction_data['transaction_type'] == "TRANSFER":
        cur.execute("""
            INSERT INTO raw.transactions (
                transaction_id, reference_number, from_account_id,
                to_account_id, amount, transaction_type, channel,
                description, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            transaction_id, 
            reference_number,
            transaction_data['from_account_id'],
            transaction_data['to_account_id'],
            transaction_data['amount'], 
            transaction_data['transaction_type'],
            transaction_data['channel'],
            description, 
            created_at
        ))
        insert_new_transaction_entries(cur = cur, transaction_data=transaction_data)
    elif transaction_data['transaction_type']== "CREDIT": # Nạp tiền
        cur.execute("""
            INSERT INTO raw.transactions (
                transaction_id, reference_number, from_account_id,
                to_account_id, amount, transaction_type, channel,
                description, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            transaction_id, 
            reference_number,
            "BANK_CASH",
            transaction_data['to_account_id'],
            transaction_data['amount'], 
            transaction_data['transaction_type'],
            transaction_data['channel'],
            description, 
            created_at
        ))
        insert_new_transaction_entries(cur = cur, transaction_data=transaction_data)
    else: #Rút tiền (DEBIT)
        cur.execute("""
            INSERT INTO raw.transactions (
                transaction_id, reference_number, from_account_id,
                to_account_id, amount, transaction_type, channel,
                description, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            transaction_id, 
            reference_number,
            transaction_data['from_account_id'],
            "BANK_CASH",
            transaction_data['amount'], 
            "DEBIT",
            "BRANCH",
            description, 
            created_at
        ))
        insert_new_transaction_entries(cur = cur, transaction_data=transaction_data)
        

def navigate_func(n=20):
    with connect_to_db() as conn:
        # Tạo connection
        cur = conn.cursor()
        # n = số transactions tạo trong 1 lần chạy DAG
        for i in range(n):
            # random 1 trong 3 kiểu transaction: TRANSFER, DEBIT, CREDIT
            transaction_type = random_transaction_type()
            match transaction_type:
                case 'DEBIT':
                    # DEBIT = Rút tiền. Tài khoản gốc bị trừ tiền, tài khoản BANK_CASH được cộng
                    account = get_one_or_two_accounts(1, cur=cur) # Chọn 1 account ngẫu nhiên để thực hiện chuyển tiền
                    from_account, from_balance = account[0] # Lấy ra account_id và balance
                    amount = int(random.uniform(1_000, from_balance)) #Random 1 số ngẫu nhiên làm amount
                    amount = round(amount, -5)
                    from_new_balance = from_balance - amount # Tính luôn new balance sau khi rút tiền
                    # Update balance mới cho tài khoản sau khi rút tiền:
                    cur.execute("""
                        UPDATE raw.accounts
                        SET balance = %s
                        WHERE account_id = %s
                    """, (from_new_balance, from_account))
                    # print(f"account {from_account} từ {from_balance} còn {from_new_balance}")
                    """
                    Với case debit, cần gửi đi mã tài khoản thực hiện rút tiền(from_account), số tiền rút(amount),
                    transaction_type = DEBIT để làm bảng raw.transactions

                    balance trước khi thực hiện transaction để làm bảng entries (from_balance)
                    """
                    transaction_data = {}
                    transaction_data['from_account_id'] = from_account
                    transaction_data['transaction_type'] = 'DEBIT'
                    transaction_data['amount'] = amount
                    transaction_data['from_account_balance_before'] = from_balance
                    transaction_data['channel'] = 'BRANCH'
                    insert_new_transaction_record(cur= cur, transaction_data=transaction_data)


                case 'CREDIT':
                    # print("Truong hop credit: nạp tiền TK")
                    account = get_one_or_two_accounts(1, cur=cur)
                    to_account, to_balance = account[0]
                    amount = int(random.uniform(1_000, 10_000_000))
                    amount = round(amount, -5)
                    to_new_balance = to_balance + amount
                    # Update balance mới cho tài khoản sau khi nạp tiền:
                    cur.execute("""
                        UPDATE raw.accounts
                        SET balance = %s
                        WHERE account_id = %s
                    """, (to_new_balance, to_account))
                    # print(f"account {to_account} từ {to_balance} lên {to_new_balance}")
                    transaction_data = {}
                    transaction_data['to_account_id'] = to_account
                    transaction_data['transaction_type'] = 'CREDIT'
                    transaction_data['amount'] = amount
                    transaction_data['to_account_balance_before'] = to_balance
                    transaction_data['channel'] = 'BRANCH'
                    insert_new_transaction_record(cur= cur, transaction_data=transaction_data)

                case 'TRANSFER':
                    # print("Chuyen tien")
                    accounts = get_one_or_two_accounts(2, cur=cur)
                    from_account, from_balance = accounts[0]
                    to_account, to_balance = accounts[1]
                    upper_limit = min(from_balance, 100_000_000)
                    if upper_limit < 1_000:
                        amount = int(upper_limit)
                    else:
                        amount = int(random.uniform(1_000, upper_limit))
                    amount = round(amount, -5)
                    from_new_balance = from_balance - amount
                    to_new_balance = to_balance + amount
                    # Update balance for A
                    cur.execute("""
                        UPDATE raw.accounts
                        SET balance = %s
                        WHERE account_id = %s
                    """, (from_new_balance, from_account))
                    # Update balance for B
                    cur.execute("""
                        UPDATE raw.accounts
                        SET balance = %s
                        WHERE account_id = %s
                    """, (to_new_balance, to_account))
                    # call function to insert transaction
                    transaction_data = {}
                    transaction_data['from_account_id'] = from_account
                    transaction_data['to_account_id'] = to_account
                    transaction_data['transaction_type'] = 'TRANSFER'
                    transaction_data['amount'] = amount
                    transaction_data['from_account_balance_before'] = from_balance
                    transaction_data['to_account_balance_before'] = to_balance
                    transaction_data['channel'] = 'MOBILE'
                    insert_new_transaction_record(cur= cur, transaction_data=transaction_data)
