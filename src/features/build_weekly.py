import pathlib
from sqlalchemy import text
from src.db.connection import get_engine

SQL_PATH = pathlib.Path("sql/02_build_weekly.sql")

def main():
    engine = get_engine()
    with engine.begin() as conn:
        source_rows = conn.execute(text("SELECT COUNT(*) FROM core.line_items")).scalar()
        print(f"   core.line_items has {source_rows} rows to aggregate.")

        conn.execute(text("TRUNCATE TABLE core.sku_weekly"))
        result = conn.execute(text("""
            INSERT INTO core.sku_weekly (
                week_start, sku_id, units_week, avg_net_price, avg_discount_pct, promo_flag_week
            )
            SELECT
                date_trunc('week', invoice_date)::date AS week_start,
                sku_id,
                SUM(units)          AS units_week,
                AVG(net_unit_price) AS avg_net_price,
                AVG(discount_pct)   AS avg_discount_pct,
                MAX(promo_flag)     AS promo_flag_week
            FROM core.line_items
            GROUP BY 1, 2
            ORDER BY 1, 2
        """))
        print(f"✅ core.sku_weekly rebuilt — {result.rowcount} rows inserted")

if __name__ == "__main__":
    main()