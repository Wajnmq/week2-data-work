from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.transforms import missingness_report
import pandas as pd

orders = pd.read_parquet("data/processed/orders.parquet")
rep = missingness_report(orders)
print(rep.head(5))