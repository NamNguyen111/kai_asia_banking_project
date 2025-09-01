INSERT INTO raw.bonds (
    bond_code, bond_name, bond_type, coupon_frequency, issuer, 
    face_value, interest_rate, issue_date, maturity_date
)
VALUES
-- Government bond 5 years, annual coupon
('GOVB001', 'Government Bond 5Y', 'Government', '12 months', 'State Treasury',
 10000000, 5.20, '2023-01-01', '2028-01-01'),

-- Government bond 10 years, semi-annual coupon
('GOVB002', 'Government Bond 10Y', 'Government', '6 months', 'State Treasury',
 10000000, 5.80, '2022-07-01', '2032-07-01'),

-- Corporate bond 3 years, semi-annual coupon
('CORP001', 'Vingroup Bond 3Y', 'Corporate', '6 months', 'Vingroup',
 1000000, 9.50, '2024-05-15', '2027-05-15'),

-- Corporate bond 7 years, annual coupon
('CORP002', 'Masan Bond 7Y', 'Corporate', '12 months', 'Masan Group',
 1000000, 10.20, '2023-09-01', '2030-09-01'),

-- Zero-coupon bond 5 years (no periodic coupon)
('ZERO001', 'Zero-coupon Bond 5Y', 'Zero-coupon', NULL, 'Techcombank',
 1000000, 0.00, '2022-01-01', '2027-01-01'),

-- Government bond 15 years, annual coupon
('GOVB003', 'Government Bond 15Y', 'Government', '12 months', 'State Treasury',
 10000000, 6.10, '2021-06-01', '2036-06-01'),

-- Corporate bond 5 years, semi-annual coupon
('CORP003', 'Vinamilk Bond 5Y', 'Corporate', '6 months', 'Vinamilk',
 1000000, 8.75, '2023-04-10', '2028-04-10'),

-- Bank bond 3 years, annual coupon
('BANK001', 'Vietcombank Bond 3Y', 'Corporate', '12 months', 'Vietcombank',
 1000000, 7.20, '2024-01-01', '2027-01-01'),

 -- Zero-coupon bond 3 years
('ZERO002', 'Zero-coupon Bond 3Y', 'Zero-coupon', NULL, 'BIDV',
 1000000, 0.00, '2023-07-01', '2026-07-01');


 