-- sql/01_create_tables.sql

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;

-- Keep staging so debugging is easy (raw-ish values from CSV Detail rows)
CREATE TABLE IF NOT EXISTS staging.invoice_detail_raw (
  id               BIGSERIAL PRIMARY KEY,
  source_file      TEXT NOT NULL,
  inv_num          TEXT,
  invoice_date_raw TEXT,     -- e.g. 02/17/2026
  sku_id_raw       TEXT,     -- from CSV column named "InvoiceDate" (contains item number)
  description_raw  TEXT,     -- CustAddr2 / Description
  brand_raw        TEXT,     -- CustAddr1 / Brand
  qty_raw          TEXT,     -- Unnamed: 29 / Servings
  reg_unit_raw     TEXT,     -- Trip / RegPricePerUnit
  net_unit_raw     TEXT,     -- ZipCode / NetPricePerUnit
  inserted_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Add brand_raw to existing staging table if it was created before this column existed
ALTER TABLE staging.invoice_detail_raw
  ADD COLUMN IF NOT EXISTS brand_raw TEXT;

-- Clean, ML-ready table (minimal + accurate)
CREATE TABLE IF NOT EXISTS core.line_items (
  id              BIGSERIAL PRIMARY KEY,
  source_file     TEXT NOT NULL,
  invoice_num     TEXT NOT NULL,
  invoice_date    DATE NOT NULL,
  sku_id          TEXT NOT NULL,
  description     TEXT,
  units           NUMERIC NOT NULL,
  reg_unit_price  NUMERIC,
  net_unit_price  NUMERIC,
  discount_pct    NUMERIC NOT NULL DEFAULT 0,
  promo_flag      INTEGER NOT NULL DEFAULT 0,


  CONSTRAINT ux_invoice_sku UNIQUE (invoice_num, sku_id)
);

-- Weekly aggregation table (used later for modeling)
CREATE TABLE IF NOT EXISTS core.sku_weekly (
  week_start       DATE NOT NULL,
  sku_id           TEXT NOT NULL,
  units_week       NUMERIC NOT NULL,
  avg_net_price    NUMERIC,
  avg_discount_pct NUMERIC,
  promo_flag_week  INTEGER NOT NULL,
  PRIMARY KEY (week_start, sku_id)
);