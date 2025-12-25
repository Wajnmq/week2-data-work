import pandas as pd

def iqr_bounds(s, k=1.5):
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k*iqr), float(q3 + k*iqr)

def winsorize(s, lo=0.01, hi=0.99):
    a, b = s.quantile(lo), s.quantile(hi)
    return s.clip(lower=a, upper=b)

orders = pd.read_parquet("data/processed/orders_clean.parquet")
s = orders["amount"].dropna()

print(s.quantile([0.5, 0.9, 0.99]))

lo, hi = iqr_bounds(s, k=1.5)
n_out = ((orders["amount"] < lo) | (orders["amount"] > hi)).sum()
print("bounds:", lo, hi)
print("n_outliers:", int(n_out))

orders2 = orders.assign(amount_winsor=winsorize(orders["amount"]))
print(orders2["amount_winsor"].min(), orders2["amount_winsor"].max())

orders2 = orders.assign(
    amount_winsor=winsorize(orders["amount"]),
    is_refund=orders["status_clean"].eq("refund")
)
print(orders2["is_refund"].value_counts(dropna=False))