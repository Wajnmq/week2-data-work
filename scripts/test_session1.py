import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
from bootcamp_data.transforms import parse_datetime, add_time_parts

orders = pd.read_parquet("data/processed/orders_clean.parquet")
orders2 = parse_datetime(orders, "created_at", utc=True)

print(orders2["created_at"].dtype)
print("n_missing_created_at:", orders2["created_at"].isna().sum())

orders3 = (
    orders.pipe(parse_datetime, col="created_at", utc=True)
          .pipe(add_time_parts, ts_col="created_at")
)

print(orders3[["created_at","month","dow","hour"]].head())