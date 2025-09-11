import json
from pathlib import Path
import pandas as pd
from .utils import setup_logger, now_iso, filename_for_date

logger = setup_logger("transform")

def transform_raw_to_silver(raw_path: str, cfg):
    raw_path = Path(raw_path)
    with open(raw_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # expectativa: data contém 'base_code' ou 'base' e 'conversion_rates' ou 'rates'
    base = data.get("base_code") or data.get("base") or data.get("base_currency")
    rates = data.get("conversion_rates") or data.get("rates") or {}

    rows = []
    for currency, rate in rates.items():
        rows.append({
            "base_currency": base,
            "currency": currency,
            "rate": float(rate),
            "fetched_at": data.get("time_last_update_utc") or data.get("time_last_update_unix") or data.get("_ingested_at") or now_iso()
        })

    df = pd.DataFrame(rows)

    # validations
    min_rate = float(cfg.get("validation", {}).get("min_rate", 1e-8))
    if (df["rate"] <= 0).any():
        bad = df[df["rate"] <= 0]
        logger.error(f"Rates <= 0 found: {len(bad)}")
        raise ValueError("Encontradas taxas com valor <= 0")

    if (df["rate"] < min_rate).any():
        warn_count = (df["rate"] < min_rate).sum()
        logger.warning(f"{warn_count} rates below min_rate threshold ({min_rate})")

    # Optionally filter allowed currencies
    allowed = cfg.get("validation", {}).get("allowed_currencies") or []
    if allowed:
        df = df[df["currency"].isin(allowed)]

    # write silver as parquet (and csv as convenience)
    silver_dir = Path(cfg["paths"]["silver"])
    out_name = filename_for_date(prefix="", ext="parquet")
    out_path = silver_dir / out_name
    df.to_parquet(out_path, index=False)
    # also csv copy
    df.to_csv(str(out_path.with_suffix(".csv")), index=False)
    logger.info(f"Wrote silver to {out_path}")
    return out_path
