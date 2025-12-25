import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

from bootcamp_data.etl import ETLConfig, run_etl

cfg = ETLConfig(
    root=ROOT,
    raw_orders=ROOT / "data" / "raw" / "orders.csv",
    raw_users=ROOT / "data" / "raw" / "users.csv",
    out_orders_clean=ROOT / "data" / "processed" / "orders_clean.parquet",
    out_users=ROOT / "data" / "processed" / "users.parquet",
    out_analytics=ROOT / "data" / "processed" / "analytics_table.parquet",
    run_meta=ROOT / "data" / "processed" / "_run_meta.json",
)

if __name__ == "__main__":
    run_etl(cfg)