INSERT INTO raw.customers (
    customer_id,
    full_name,
    gender,
    income_range,
    occupation,
    phone, 
    email,
    id_number,
    address,
    date_of_birth,
    status,
    customer_segment,
    customer_since,
    created_at
) VALUES (
    'BANK00000001',
    'KAI_ASIA_CUSTOMER',
    '0',
    '1',
    'Bank Owner',
    '0888888888',
    'bank_system@gmail.com',
    '015203002515',
    'Hanoi',
    '2000-01-01',
    'ACTIVE',
    'PRIVATE',
    '2000-07-15',
    NOW()
);


INSERT INTO raw.accounts (
    account_id,
    customer_id,
    account_number,
    account_type,
    balance,
    status,
    branch_id,
    created_at
) VALUES (
    'BANK_CASH',
    'BANK00000001',
    '000000000000BANK',
    'CASH',
    999999999900000000.99,
    'ACTIVE',
    NULL,
    NOW()
);
