import pandas as pd
from pathlib import Path

# قراءة الملف
ROOT = Path(__file__).resolve().parents[1]
orders = pd.read_parquet(ROOT / "data/processed/orders.parquet")

# تطبيع النص (normalize)
import re
_ws = re.compile(r"\s+")

def normalize_text(s):
    return (
        s.astype("string")
         .str.strip()
         .str.casefold()
         .str.replace(_ws, " ", regex=True)
    )

status_norm = normalize_text(orders["status"])

# ✅ الـ mapping (حلك هنا)
mapping = {
    "paid": "paid",
    "refund": "refund",
    "refunded": "refund",
    "payment_received": "paid",
    "cancelled": "refund",
}

# تطبيق الـ mapping
status_clean = status_norm.apply(lambda x: mapping.get(x, x))

# طباعة النتيجة
print("Before normalization:")
print(orders["status"].unique())
print("\nAfter normalization:")
print(status_norm.unique())
print("\nAfter mapping:")
print(status_clean.unique())