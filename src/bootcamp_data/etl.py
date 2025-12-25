from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import logging
import json

import pandas as pd

from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
)
from bootcamp_data.transforms import (
    enforce_schema,
    add_missing_flags,
    normalize_text,
    apply_mapping,
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag,
)
from bootcamp_data.joins import safe_left_join

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ETLConfig:
    """Configuration for the ETL pipeline."""
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path


def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load raw CSV files."""
    log.info("Loading raw orders from %s", cfg.raw_orders)
    orders = read_orders_csv(cfg.raw_orders)
    
    log.info("Loading raw users from %s", cfg.raw_users)
    users = read_users_csv(cfg.raw_users)
    
    log.info("Loaded orders: %d rows, users: %d rows", len(orders), len(users))
    return orders, users


def transform(orders_raw: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    """Transform raw data into analytics table."""
    
    
    log.info("Validating inputs...")
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")
    
   
    log.info("Enforcing schema...")
    orders = enforce_schema(orders_raw)
    
    
    log.info("Normalizing status...")
    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    orders = orders.assign(status_clean=apply_mapping(status_norm, mapping))
    
    
    log.info("Adding missing flags...")
    orders = orders.pipe(add_missing_flags, cols=["amount", "quantity"])
    
    
    log.info("Parsing datetime...")
    orders = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )
    
    
    log.info("Joining orders with users...")
    joined = safe_left_join(orders, users, on="user_id", validate="many_to_one", suffixes=("", "_user"))
    
    
    if len(joined) != len(orders):
        log.error("Row count changed after join: %d -> %d", len(orders), len(joined))
        raise AssertionError("Join explosion detected")
    
    
    log.info("Winsorizing amount...")
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)
    
    log.info("Transformation complete: %d rows", len(joined))
    return joined


def load_outputs(analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    """Write processed data to disk."""
    log.info("Writing users to %s", cfg.out_users)
    write_parquet(users, cfg.out_users)
    
    log.info("Writing analytics table to %s", cfg.out_analytics)
    write_parquet(analytics, cfg.out_analytics)


def write_run_meta(cfg: ETLConfig, *, analytics: pd.DataFrame) -> None:
    """Write run metadata JSON."""
    missing_created_at = int(analytics["created_at"].isna().sum())
    country_match_rate = 1.0 - float(analytics["country"].isna().mean())
    
    meta = {
        "rows_out": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": round(country_match_rate, 4),
        "config": {k: str(v) for k, v in cfg.__dict__.items()},
    }
    
    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    log.info("Wrote run metadata to %s", cfg.run_meta)


def run_etl(cfg: ETLConfig) -> None:
    """Main ETL entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s"
    )
    
    log.info("Starting ETL pipeline...")
    
    
    orders_raw, users = load_inputs(cfg)
    
    
    analytics = transform(orders_raw, users)
    
    
    load_outputs(analytics, users, cfg)
    
    
    write_run_meta(cfg, analytics=analytics)
    
    log.info("ETL pipeline completed successfully!")