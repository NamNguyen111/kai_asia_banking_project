-- Source -> stag
-- Dữ liệu từ nguồn em đẩy vào stag
-- Trong stag phải có cột để biết đc nó là ngày dữ liệu nào và thời điểm nào mình đẩy vào
-- Bọn anh thường dùng data_date hoặc business_date làm ngày dữ liệu
-- etl_at làm thời điểm đẩy vào

-- 1. Bảng Khách hàng
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id VARCHAR(20) PRIMARY KEY, --ma khach hang
    full_name VARCHAR(100) NOT NULL, --ten khach hang
    phone VARCHAR(15) NOT NULL, --so dien thoai
    email VARCHAR(100), --email khach hang
    id_number VARCHAR(20) UNIQUE NOT NULL, --so CMND/CCCD
    address TEXT, --dia chi khach hang
    date_of_birth DATE, --ngay sinh
    status VARCHAR(10) DEFAULT 'ACTIVE', -- ACTIVE, BLOCKED
    created_at TIMESTAMP
);

-- 2. Bảng Tài khoản
CREATE TABLE IF NOT EXISTS raw.accounts (
    account_id VARCHAR(20) PRIMARY KEY, --ma account
    customer_id VARCHAR(20) NOT NULL, --ma khach hang, ref customer(customer_id)
    account_number VARCHAR(20) UNIQUE NOT NULL, --ma so tai khoan (vi du Techcombank 15 2003 6886 NGUYEN DUC NAM)
    account_type VARCHAR(15) NOT NULL, -- SAVINGS, CHECKING loai account. checking = thanh toan, saving = tiet kiem
    balance DECIMAL(20,2) DEFAULT 0.00, -- so du tai khoan VD: 50 000 000 000
    status VARCHAR(10) DEFAULT 'ACTIVE', -- ACTIVE, BLOCKED, CLOSED, trang thai tai khoan
    branch_id VARCHAR(10),
    created_at TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES raw.customers(customer_id), -- 1 customer_id co the co nhieu accounts
    FOREIGN KEY (branch_id) REFERENCES raw.branches(branch_id)
);

-- 3. Bảng Giao dịch
CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id UUID PRIMARY KEY,
    reference_number VARCHAR(50) UNIQUE NOT NULL, -- Mã tham chiếu cho khách hàng (cai nay la Ma Giao Dich)
    from_account_id VARCHAR(20), -- Tài khoản chuyển
    to_account_id VARCHAR(20),   -- Tài khoản nhận
    amount DECIMAL(15,2) NOT NULL,
    transaction_type VARCHAR(20) DEFAULT 'TRANSFER',
    channel VARCHAR(15) NOT NULL, --MOBILE, ATM, BRANCH
    description TEXT,
    created_at TIMESTAMP,
    FOREIGN KEY (from_account_id) REFERENCES raw.accounts(account_id),
    FOREIGN KEY (to_account_id) REFERENCES raw.accounts(account_id)
);

-- 4. Bảng Bút toán kép (Double Entry Bookkeeping) DEBIT = trừ tiền, CREDIT = ting ting (cộng tiền)
CREATE TABLE IF NOT EXISTS raw.transaction_entries (
    entry_id UUID PRIMARY KEY,
    transaction_id UUID NOT NULL,
    account_id VARCHAR(20) NOT NULL, -- Tài khoản bị ảnh hưởng bởi giao dịch đó, với mỗi transaction sẽ sinh ra 2 entries
    debit_amount DECIMAL(15,2) DEFAULT 0.00,  -- Số tiền ghi nợ (tiền RA)
    credit_amount DECIMAL(15,2) DEFAULT 0.00, -- Số tiền ghi có (tiền VÀO)
    balance_before DECIMAL(20,2) NOT NULL,    -- Số dư trước giao dịch
    balance_after DECIMAL(20,2) NOT NULL,     -- Số dư sau giao dịch
    entry_type VARCHAR(10) NOT NULL,          -- DEBIT hoặc CREDIT
    entry_sequence INT NOT NULL,              -- Thứ tự bút toán (1, 2, 3...) Trong project này làm sequence 1,2 thôi
    description TEXT,
    created_at TIMESTAMP,
    FOREIGN KEY (transaction_id) REFERENCES raw.transactions(transaction_id),
    FOREIGN KEY (account_id) REFERENCES raw.accounts(account_id)
);

-- 5. Bảng Ngân hàng trong nước
CREATE TABLE IF NOT EXISTS raw.branches (
    branch_id VARCHAR(10) PRIMARY KEY,
    branch_name TEXT NOT NULL, 
    address TEXT,       
    status VARCHAR(10) DEFAULT 'ACTIVE',
    created_at TIMESTAMP
);