import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
orders = pd.read_parquet(ROOT / "data/processed/orders.parquet")

import re
_ws = re.compile(r"\s+")
status_norm = (
    orders["status"]
    .astype("string")
    .str.strip()
    .str.casefold()
    .str.replace(_ws, " ", regex=True)
)

mapping = {
    "paid": "paid",
    "refund": "refund",
    "refunded": "refund",  
}
status_clean = status_norm.apply(lambda x: mapping.get(x, x))

print(status_clean.unique())