from pathlib import Path
import pandas as pd
from .utils import setup_logger, filename_for_date
import os

logger = setup_logger("load")

def load_to_gold(silver_parquet_path: str, cfg):
    silver_parquet_path = Path(silver_parquet_path)
    df = pd.read_parquet(silver_parquet_path)
    gold_dir = Path(cfg["paths"]["gold"])
    out_name = silver_parquet_path.name  # keep same name
    out_path = gold_dir / out_name
    df.to_parquet(out_path, index=False)
    logger.info(f"Wrote gold parquet to {out_path}")
    return out_path

def load_to_db(gold_parquet_path: str, cfg):
    if not cfg.get("db", {}).get("enabled", False):
        logger.info("DB load disabled in config")
        return

    from sqlalchemy import create_engine
    engine = create_engine(cfg["db"]["url"], future=True)
    table = cfg["db"].get("table_name", "fx_rates")
    df = pd.read_parquet(gold_parquet_path)

    # ensure types
    df['rate'] = df['rate'].astype(float)
    df.to_sql(table, engine, if_exists='append', index=False, method='multi')
    logger.info(f"Inserted {len(df)} rows into {table}")
