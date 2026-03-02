import pathlib
from sqlalchemy import text
from src.db.connection import get_engine
from src.ingestion.load_invoice_csv import main as load_one
from src.features.build_weekly import main as build_weekly

DATA_PATH = pathlib.Path("data/raw")


def _reset_tables(engine):
    """Wipe all ingested data and reset BIGSERIAL sequences back to 1."""
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE core.sku_weekly"))
        conn.execute(text("TRUNCATE TABLE core.line_items RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE staging.invoice_detail_raw RESTART IDENTITY CASCADE"))
    print("🗑️  Tables cleared, sequences reset to 1.\n")


def main():
    csv_files = sorted(DATA_PATH.glob("*.csv"))

    if not csv_files:
        print("No CSV files found in data/raw/")
        return

    engine = get_engine()
    _reset_tables(engine)

    print(f"Found {len(csv_files)} invoice files.\n")

    failed = []
    for csv_file in csv_files:
        print(f"Loading {csv_file.name}...")
        try:
            load_one(str(csv_file))
        except Exception as e:
            print(f"⚠️  Error loading {csv_file.name} — skipping. Reason: {e}")
            failed.append(csv_file.name)

    if failed:
        print(f"\n⚠️  Completed with {len(failed)} failed file(s): {', '.join(failed)}")
    else:
        print("\n✅ All invoices loaded successfully.")

    print("\nBuilding weekly aggregates...")
    build_weekly()

if __name__ == "__main__":
    main()