--optional but nice
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;

--1) Staging table: holds the Detail rows after we filter them

CREATE TABLE IF NOT EXISTS staging.invoice_detail_raw (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    inv_num TEXT,
    invoice_date_raw TEXT,
    sku_id_raw TEXT,
    upc_raw TEXT,
    brand_raw TEXT,
    description_raw TEXT,
    pack_size_raw TEXT,
    unit_type_raw TEXT,
    qty_raw TEXT,
    net_unit_raw TEXT,
    discount_raw TEXT,
    line_total_raw TEXT,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);

--2) Clean table: typed columns for ML/analytics
CREATE TABLE IF NOT EXISTS core.line_items (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    invoice_num TEXT NOT NULL,
    invoice_date DATE NOT NULL,
    sku_id TEXT NOT NULL,
    upc_code TEXT,
    brand TEXT,
    description TEXT,
    pack_size TEXT,
    unit_type TEXT,
    units NUMERIC NOT NULL,
    net_unit_price NUMERIC,
    discount_amount NUMERIC DEFAULT 0,
    line_total NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

--3) Weekly aggregation (later used for training)
CREATE TABLE IF NOT EXISTS core.sku_weekly (
    week_start DATE NOT NULL,
    sku_id TEXT NOT NULL,
    units_week NUMERIC NOT NULL,
    avg_price_week NUMERIC,
    avg_discount_week NUMERIC,
    promo_flag_week INTEGER NOT NULL,
    PRIMARY KEY (week_start, sku_id)
);