from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.transforms import add_missing_flags
import pandas as pd

orders = pd.read_parquet("data/processed/orders.parquet")
orders2 = add_missing_flags(orders, ["amount", "quantity"])
output = orders2[["amount", "amount__isna", "quantity", "quantity__isna"]]
print(output.head())