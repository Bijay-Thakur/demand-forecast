-- Run this ONCE to wipe all loaded data and reset sequences to 1.
-- After running this, re-run the ingestion script to reload from CSVs.

TRUNCATE TABLE core.sku_weekly;
TRUNCATE TABLE core.line_items RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.invoice_detail_raw RESTART IDENTITY CASCADE;
