-- 1. Bảng Khách hàng
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id VARCHAR(20) PRIMARY KEY, --ma khach hang
    full_name VARCHAR(100) NOT NULL, --ten khach hang
    gender BIT NOT NULL,  
    income_range VARCHAR(10),   -- Thu nhập một năm (annual income)
    occupation VARCHAR(30), -- Nghề nghiệp
    phone VARCHAR(15) NOT NULL, --so dien thoai
    email VARCHAR(100), --email khach hang
    id_number VARCHAR(20) UNIQUE NOT NULL, --so CMND/CCCD
    address TEXT, --dia chi khach hang
    date_of_birth DATE, --ngay sinh
    status VARCHAR(10) DEFAULT 'ACTIVE', -- ACTIVE, BLOCKED
    customer_segment VARCHAR(10), 
    customer_since DATE,  -- Ngày trở thành khách hàng 
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
    channel VARCHAR(10) NOT NULL, --MOBILE, ATM, BRANCH
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

-- 5. Bảng chi nhánh
CREATE TABLE IF NOT EXISTS raw.branches (
    branch_id VARCHAR(10) PRIMARY KEY,
    branch_name TEXT NOT NULL, 
    address TEXT,       
    status VARCHAR(10) DEFAULT 'ACTIVE',
    created_at TIMESTAMP
);

-- 6. Bảng Trái phiếu
CREATE TABLE IF NOT EXISTS raw.bonds (
    bond_code VARCHAR(10) PRIMARY KEY, -- Mã trái phiếu
    bond_name VARCHAR(255) NOT NULL, -- Tên mã
    bond_type VARCHAR(20), -- Loại trái phiếu(Chính phủ, doanh nghiệp, Zero-coupon,...)
    coupon_frequency VARCHAR(20), -- Kỳ hạn trả lãi: 6 tháng, 12 tháng
    issuer VARCHAR(50) NOT NULL, -- Đơn vị phát hành
    face_value NUMERIC(18,2) NOT NULL, -- Mệnh giá
    interest_rate NUMERIC(5,2) NOT NULL, -- Lãi suất/năm
    issue_date DATE NOT NULL,   -- Ngày phát hành
    maturity_date DATE NOT NULL -- Ngày đáo hạn
);


CREATE TABLE IF NOT EXISTS raw.bond_holdings (
    bond_transaction_id UUID PRIMARY KEY,
    bond_code VARCHAR(10), -- Mã của trái phiếu
    customer_id VARCHAR(20) NOT NULL, -- Mã khách hàng
    buy_quantity INT NOT NULL,  -- Số lượng trái phiếu đã mua
    status VARCHAR(50) DEFAULT 'ACTIVE',   -- ACTIVE, MATURED, REDEEMED
    created_at TIMESTAMP DEFAULT NOW(),

    FOREIGN KEY (customer_id) REFERENCES raw.customers(customer_id),
    FOREIGN KEY (bond_code) REFERENCES raw.bonds(bond_code)
);

-- 7. Bảng Chứng chỉ quỹ
CREATE TABLE IF NOT EXISTS raw.fund_certificate_holdings (
    fund_cert_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    fund_name VARCHAR(255) NOT NULL,
    units INT NOT NULL,                    -- số lượng chứng chỉ quỹ
    unit_price NUMERIC(18,2) NOT NULL,     -- giá 1 chứng chỉ tại thời điểm mua
    purchase_date DATE NOT NULL,
    status VARCHAR(10) DEFAULT 'ACTIVE',   -- ACTIVE, REDEEMED, EXPIRED
    created_at TIMESTAMP DEFAULT NOW(),

    FOREIGN KEY (customer_id) REFERENCES raw.customers(customer_id)
);

-- 8. Bảng Tiền gửi có kỳ hạn
CREATE TABLE IF NOT EXISTS raw.term_deposit_holdings (
    term_deposit_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    principal NUMERIC(18,2) NOT NULL,       -- số tiền gửi
    interest_rate NUMERIC(5,2) NOT NULL,
    start_date DATE NOT NULL,
    maturity_date DATE NOT NULL,
    status VARCHAR(10) DEFAULT 'ACTIVE',    -- ACTIVE, CLOSED, MATURED
    created_at TIMESTAMP DEFAULT NOW(),

    FOREIGN KEY (customer_id) REFERENCES raw.customers(customer_id)
);

-- 9. Bảng Chứng chỉ tiền gửi
CREATE TABLE IF NOT EXISTS raw.certificate_of_deposit_holdings (
    cd_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    amount NUMERIC(18,2) NOT NULL,          -- số tiền mua
    interest_rate NUMERIC(5,2) NOT NULL,
    issue_date DATE NOT NULL,
    maturity_date DATE NOT NULL,
    status VARCHAR(50) DEFAULT 'ACTIVE',    -- ACTIVE, REDEEMED, MATURED
    created_at TIMESTAMP DEFAULT NOW(),

    FOREIGN KEY (customer_id) REFERENCES raw.customers(customer_id)
);

-- 10. Bảng Nợ (Khoản vay)
CREATE TABLE IF NOT EXISTS raw.loans (
    loan_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    loan_amount NUMERIC(18,2) NOT NULL,     -- số tiền vay
    interest_rate NUMERIC(5,2) NOT NULL,
    start_date DATE NOT NULL,
    maturity_date DATE NOT NULL,
    collateral VARCHAR(255),                -- tài sản thế chấp (nếu có)
    status VARCHAR(50) DEFAULT 'ONGOING',   -- ONGOING, OVERDUE, CLOSED, DEFAULTED
    created_at TIMESTAMP DEFAULT NOW(),

    FOREIGN KEY (customer_id) REFERENCES raw.customers(customer_id)
);

-- 11. Bảng Lịch sử trả nợ
CREATE TABLE IF NOT EXISTS raw.loan_repayments (
    repayment_id SERIAL PRIMARY KEY,
    loan_id INT NOT NULL,
    repayment_date DATE NOT NULL,
    amount_paid NUMERIC(18,2) NOT NULL,
    remaining_balance NUMERIC(18,2) NOT NULL,
    status VARCHAR(50) DEFAULT 'ON_TIME',   -- ON_TIME, LATE, PARTIAL
    created_at TIMESTAMP DEFAULT NOW(),

    FOREIGN KEY (loan_id) REFERENCES raw.loans(loan_id)
);



