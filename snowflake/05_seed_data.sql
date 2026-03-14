-- ============================================================
-- 05 — Seed Data: Zimbabwe Suburbs Reference
-- Run once after table creation
-- ============================================================

USE DATABASE ZIM_PROPERTY_DB;
USE SCHEMA STAGING;

INSERT INTO ZW_SUBURBS_REFERENCE (suburb_name, suburb_name_clean, city, province, latitude_approx, longitude_approx)
VALUES
-- ── Harare ────────────────────────────────────────────────
('Borrowdale',          'borrowdale',       'Harare', 'Harare',      -17.7476, 31.1085),
('Borrowdale Brooke',   'borrowdale_brooke','Harare', 'Harare',      -17.7300, 31.1200),
('Glen Lorne',          'glen_lorne',       'Harare', 'Harare',      -17.7600, 31.1300),
('Highlands',           'highlands',        'Harare', 'Harare',      -17.8072, 31.0653),
('Avondale',            'avondale',         'Harare', 'Harare',      -17.7900, 31.0400),
('Greendale',           'greendale',        'Harare', 'Harare',      -17.8153, 31.1058),
('Mount Pleasant',      'mount_pleasant',   'Harare', 'Harare',      -17.7750, 31.0520),
('Alexandra Park',      'alexandra_park',   'Harare', 'Harare',      -17.8000, 31.0350),
('Chisipite',           'chisipite',        'Harare', 'Harare',      -17.7800, 31.1200),
('Greystone Park',      'greystone_park',   'Harare', 'Harare',      -17.7540, 31.1400),
('Tynwald',             'tynwald',          'Harare', 'Harare',      -17.8200, 31.0000),
('Harare CBD',          'harare_cbd',       'Harare', 'Harare',      -17.8292, 31.0522),
('Eastlea',             'eastlea',          'Harare', 'Harare',      -17.8300, 31.0900),
('Msasa',               'msasa',            'Harare', 'Harare',      -17.8500, 31.1300),
('Hatfield',            'hatfield',         'Harare', 'Harare',      -17.8600, 31.0800),
('Pomona',              'pomona',           'Harare', 'Harare',      -17.7860, 31.1440),
('Gunhill',             'gunhill',          'Harare', 'Harare',      -17.7700, 31.0900),
('Mabelreign',          'mabelreign',       'Harare', 'Harare',      -17.8150, 30.9980),
('Westgate',            'westgate',         'Harare', 'Harare',      -17.8000, 30.9800),
('Waterfalls',          'waterfalls',       'Harare', 'Harare',      -17.8800, 31.0300),
('Kuwadzana',           'kuwadzana',        'Harare', 'Harare',      -17.8200, 30.9500),
('Dzivarasekwa',        'dzivarasekwa',     'Harare', 'Harare',      -17.8350, 30.9350),
('Budiriro',            'budiriro',         'Harare', 'Harare',      -17.8900, 30.9700),
('Glen View',           'glen_view',        'Harare', 'Harare',      -17.9100, 30.9900),

-- ── Bulawayo ──────────────────────────────────────────────
('Bulawayo CBD',        'bulawayo_cbd',     'Bulawayo', 'Matabeleland North', -20.1500, 28.5800),
('Hillside',            'hillside_byo',     'Bulawayo', 'Matabeleland North', -20.1700, 28.5600),
('Suburbs',             'suburbs_byo',      'Bulawayo', 'Matabeleland North', -20.1400, 28.5500),
('Burnside',            'burnside',         'Bulawayo', 'Matabeleland North', -20.1800, 28.5300),
('Famona',              'famona',           'Bulawayo', 'Matabeleland North', -20.1600, 28.5200),
('Matsheumhlope',       'matsheumhlope',    'Bulawayo', 'Matabeleland North', -20.1500, 28.5400),
('Queens Park',         'queens_park',      'Bulawayo', 'Matabeleland North', -20.1900, 28.5800),
('Northend',            'northend',         'Bulawayo', 'Matabeleland North', -20.1300, 28.5900),

-- ── Mutare ────────────────────────────────────────────────
('Mutare CBD',          'mutare_cbd',       'Mutare', 'Manicaland',   -18.9700, 32.6600),
('Greenside',           'greenside_mut',    'Mutare', 'Manicaland',   -18.9600, 32.6400),
('Morningside',         'morningside_mut',  'Mutare', 'Manicaland',   -18.9800, 32.6700),

-- ── Gweru ─────────────────────────────────────────────────
('Gweru CBD',           'gweru_cbd',        'Gweru', 'Midlands',      -19.4500, 29.8200),
('Ascot',               'ascot',            'Gweru', 'Midlands',      -19.4400, 29.8100),
('Windsor Park',        'windsor_park',     'Gweru', 'Midlands',      -19.4600, 29.8000),

-- ── Norton / Ruwa ─────────────────────────────────────────
('Norton CBD',          'norton_cbd',       'Norton', 'Mashonaland West', -17.8800, 30.7000),
('Ruwa',                'ruwa',             'Ruwa', 'Mashonaland East',   -17.8900, 31.2400)
ON CONFLICT (suburb_name_clean, city) DO NOTHING;


-- ── Initial exchange rate seed (update daily via pipeline) ─
INSERT INTO ZWL_USD_EXCHANGE_RATES (rate_date, zwl_per_usd, source)
VALUES
    ('2024-01-01', 3500.00, 'RBZ'),
    ('2024-02-01', 5000.00, 'RBZ'),
    ('2024-03-01', 7800.00, 'RBZ'),
    ('2024-04-01', 13000.00,'RBZ'),
    ('2024-06-01', 25000.00,'RBZ'),   -- ZiG transition approximate
    ('2025-01-01', 27.50,    'RBZ'),  -- ZiG (new currency, reset)
    ('2025-06-01', 28.10,    'RBZ'),
    ('2026-01-01', 29.00,    'RBZ')
ON CONFLICT (rate_date) DO NOTHING;
