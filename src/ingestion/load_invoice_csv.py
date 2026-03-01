# src/ingestion/load_invoice_csv.py
import argparse
import os
import pandas as pd
from sqlalchemy import text
from src.db.connection import get_engine

# Mapping for YOUR vendor CSV (based on your sample invoice)
COLUMN_MAP = {
    "InvNum": "inv_num",
    "Unnamed: 28": "invoice_date_raw",
    "InvoiceDate": "sku_id_raw",      # yes, weird column name
    "OrderNumber": "upc_raw",
    "CustAddr1": "brand_raw",
    "CustAddr2": "description_raw",
    "CustomerName": "pack_size_raw",
    "CustomerNumber": "unit_type_raw",
    "Unnamed: 29": "qty_raw",
    "ZipCode": "net_unit_raw",
    "Whse": "discount_raw",
    "PhoneNumber": "line_total_raw",
}

NUMERIC_COLS = ["qty_raw", "net_unit_raw", "discount_raw", "line_total_raw"]

def parse_numeric(series: pd.Series) -> pd.Series:
    # Handles blanks, strings, commas, etc.
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce"
    )

def main(csv_path: str):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    # 1) Keep only true detail rows
    #    Also drop the duplicated header row sometimes embedded in the file.
    df = df[df["RecType"].astype(str).str.strip().eq("Detail")]
    if df.empty:
        raise ValueError("No RecType == 'Detail' rows found. Check the CSV export.")

    # 2) Select needed columns (only those present)
    cols_present = [c for c in COLUMN_MAP.keys() if c in df.columns]
    df = df[cols_present].rename(columns={c: COLUMN_MAP[c] for c in cols_present})

    # 3) Add source_file
    df["source_file"] = os.path.basename(csv_path)

    # 4) Clean numeric fields
    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = parse_numeric(df[c])

    # 5) Basic date parse (invoice date)
    # Invoice date is usually MM/DD/YYYY in your export.
    df["invoice_date"] = pd.to_datetime(df["invoice_date_raw"], errors="coerce").dt.date

    # 6) Minimal sanity checks
    required = ["source_file", "inv_num", "invoice_date", "sku_id_raw", "qty_raw"]
    missing_required = [c for c in required if c not in df.columns]
    if missing_required:
        raise ValueError(f"Missing required fields after mapping: {missing_required}")

    if df["invoice_date"].isna().any():
        bad = df[df["invoice_date"].isna()].head(5)
        raise ValueError(f"Found unparseable invoice_date values. Sample:\n{bad[['invoice_date_raw']]}")

    # If quantity is missing, drop those rows (invoice exports sometimes include weird lines)
    df = df.dropna(subset=["qty_raw", "sku_id_raw"]).copy()
    if df.empty:
        raise ValueError("All detail rows dropped after removing missing qty/sku. Check mapping.")

    engine = get_engine()

    # 7) Insert into staging (raw-ish)
    staging_cols = [
        "source_file", "inv_num", "invoice_date_raw", "sku_id_raw", "upc_raw",
        "brand_raw", "description_raw", "pack_size_raw", "unit_type_raw",
        "qty_raw", "net_unit_raw", "discount_raw", "line_total_raw",
    ]
    for c in staging_cols:
        if c not in df.columns:
            df[c] = None

    df[staging_cols].to_sql(
        "invoice_detail_raw",
        con=engine,
        schema="staging",
        if_exists="append",
        index=False,
        method="multi"
    )

    # 8) Upsert into core.line_items (typed)
    # We insert using SQL so Postgres types are respected and duplicates can be ignored.
    insert_sql = text("""
        INSERT INTO core.line_items (
            source_file, invoice_num, invoice_date, sku_id, upc_code,
            brand, description, pack_size, unit_type,
            units, net_unit_price, discount_amount, line_total
        )
        SELECT
             source_file,
        inv_num AS invoice_num,
        to_date(invoice_date_raw, 'MM/DD/YYYY') AS invoice_date,
        sku_id_raw AS sku_id,
        upc_raw AS upc_code,
        brand_raw AS brand,
        description_raw AS description,
        pack_size_raw AS pack_size,
        unit_type_raw AS unit_type,
        COALESCE(qty_raw, 0) AS units,
        net_unit_raw AS net_unit_price,
        COALESCE(discount_raw, 0) AS discount_amount,
        line_total_raw AS line_total
    FROM staging.invoice_detail_raw
    WHERE source_file = :source_file
    ON CONFLICT DO NOTHING;
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, {"source_file": os.path.basename(csv_path)})

    print(f"✅ Loaded invoice detail rows from: {csv_path}")
    print("Next: run validation SQL checks (I’ll give you those right below).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to invoice CSV file")
    args = parser.parse_args()
    main(args.csv)