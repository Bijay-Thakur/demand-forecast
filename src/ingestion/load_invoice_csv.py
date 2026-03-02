# src/ingestion/load_invoice_csv.py
import argparse
import os
import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from src.db.connection import get_engine


def _insert_ignore_duplicates(table, conn, keys, data_iter):
    """to_sql helper: silently skip rows that violate a unique constraint."""
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = pg_insert(table.table).values(data).on_conflict_do_nothing()
    result = conn.execute(stmt)
    return result.rowcount

# CSV has two different header rows (row 0 = invoice-header columns, row 2 = detail columns).
# pandas uses row 0 as column names, so Detail rows are accessed via the row-0 name
# that occupies the same column position as the Detail-row field below.
#
#  pandas col name  │ actual Detail field
#  ─────────────────┼────────────────────
#  InvNum           │ InvNum
#  Unnamed: 28      │ InvoiceDate
#  InvoiceDate      │ ProductID   (yes, weird positional clash)
#  CustAddr1        │ Brand
#  CustAddr2        │ Description
#  Unnamed: 29      │ Servings    (treated as units)
#  Trip             │ RegPricePerUnit
#  ZipCode          │ NetPricePerUnit
COLUMN_MAP = {
    "InvNum":       "inv_num",
    "Unnamed: 28":  "invoice_date_raw",
    "InvoiceDate":  "sku_id_raw",
    "CustAddr1":    "brand_raw",
    "CustAddr2":    "description_raw",
    "Unnamed: 29":  "qty_raw",
    "Trip":         "reg_unit_raw",
    "ZipCode":      "net_unit_raw",
}

NUMERIC_COLS = ["qty_raw", "reg_unit_raw", "net_unit_raw"]


def parse_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce"
    )


def main(csv_path: str):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    source_file = os.path.basename(csv_path)
    engine = get_engine()

    # Early-exit if this file is already loaded into the core table
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT COUNT(*) FROM core.line_items WHERE source_file = :sf"),
            {"sf": source_file}
        ).scalar()
    if existing:
        print(f"ℹ️  Data already stored for: {source_file} ({existing} rows). Skipping.")
        return

    df = pd.read_csv(csv_path)

    # 1) Keep only true item lines
    df = df[df["RecType"].astype(str).str.strip().eq("Detail")].copy()
    if df.empty:
        raise ValueError("No RecType == 'Detail' rows found. Check the CSV export.")

    # 2) Select + rename needed columns
    cols_present = [c for c in COLUMN_MAP.keys() if c in df.columns]
    df = df[cols_present].rename(columns={c: COLUMN_MAP[c] for c in cols_present})

    # 3) Source file
    df["source_file"] = source_file

    # 4) Parse numeric fields in Python (safe)
    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = parse_numeric(df[c])

    # 5) Parse invoice date (MM/DD/YYYY)
    df["invoice_date"] = pd.to_datetime(df["invoice_date_raw"], errors="coerce").dt.date

    # 6) Sanity checks / cleanup
    required = ["source_file", "inv_num", "invoice_date", "sku_id_raw", "qty_raw", "reg_unit_raw", "net_unit_raw"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required fields after mapping: {missing}")

    df = df.dropna(subset=["inv_num", "invoice_date", "sku_id_raw", "qty_raw"]).copy()

    # 7) Compute discount_pct as an integer percentage (0-100)
    reg = df["reg_unit_raw"]
    net = df["net_unit_raw"]

    df["discount_pct"] = 0.0
    valid_reg = reg.notna() & (reg > 0) & net.notna()
    df.loc[valid_reg, "discount_pct"] = (reg[valid_reg] - net[valid_reg]) / reg[valid_reg] * 100

    df["discount_pct"] = df["discount_pct"].clip(lower=0).round(0).astype(int)

    # 8) promo_flag rule: discount > 30 %
    df["promo_flag"] = (df["discount_pct"] > 30).astype(int)

    # Clear staging for this file (idempotent)
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM staging.invoice_detail_raw WHERE source_file = :sf"),
            {"sf": source_file}
        )

    # 9) Insert into staging (raw values)
    staging_cols = [
        "source_file", "inv_num", "invoice_date_raw", "sku_id_raw",
        "description_raw", "brand_raw", "qty_raw", "reg_unit_raw", "net_unit_raw"
    ]
    # Only include columns that are actually present (brand_raw may be missing in some exports)
    staging_cols = [c for c in staging_cols if c in df.columns]
    df[staging_cols].to_sql(
        "invoice_detail_raw",
        con=engine,
        schema="staging",
        if_exists="append",
        index=False,
        method="multi"
    )

    # 10) Insert into core using CLEAN Python values (no SQL casting needed)
    core_df = df[[
        "source_file", "inv_num", "invoice_date", "sku_id_raw",
        "description_raw", "qty_raw", "reg_unit_raw", "net_unit_raw",
        "discount_pct", "promo_flag"
    ]].rename(columns={
        "inv_num":       "invoice_num",
        "sku_id_raw":    "sku_id",
        "description_raw": "description",
        "qty_raw":       "units",
        "reg_unit_raw":  "reg_unit_price",
        "net_unit_raw":  "net_unit_price"
    })

    before = core_df.shape[0]
    core_df.to_sql(
        "line_items",
        con=engine,
        schema="core",
        if_exists="append",
        index=False,
        method=_insert_ignore_duplicates
    )

    with engine.connect() as conn:
        inserted = conn.execute(
            text("SELECT COUNT(*) FROM core.line_items WHERE source_file = :sf"),
            {"sf": source_file}
        ).scalar()
    skipped = before - inserted
    if skipped > 0:
        print(f"⚠️  Duplicates found, skipped {skipped} row(s) — moving to next.")

    print(f"✅ Loaded: {source_file} ({inserted} rows inserted)")
    print("✅ discount_pct stored as integer percentage (0-100)")
    print("✅ promo_flag set where discount_pct > 30")
    print("✅ brand_raw captured in staging")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to invoice CSV file")
    args = parser.parse_args()
    main(args.csv)
