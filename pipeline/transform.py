import json
import pandas as pd
from pathlib import Path
from datetime import datetime


def now_iso():
    return datetime.utcnow().isoformat()


def transform_raw_to_silver(raw_path: str, output_path: str) -> str:
    """
    Converte JSON raw para parquet silver.

    Args:
        raw_path (str): Caminho do arquivo JSON (raw).
        output_path (str): Caminho onde salvar o arquivo parquet (silver).

    Returns:
        str: Caminho final do arquivo salvo.
    """
    raw_path = Path(raw_path)
    with open(raw_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    base = data.get("base_code") or data.get("base") or data.get("base_currency")
    rates = data.get("conversion_rates") or data.get("rates") or {}

    rows = []
    for currency, rate in rates.items():
        rows.append({
            "base_currency": base,
            "currency": currency,
            "rate": float(rate),
            "fetched_at": data.get("time_last_update_utc")
                or data.get("time_last_update_unix")
                or data.get("_ingested_at")
                or now_iso()
        })

    df = pd.DataFrame(rows)
    df.to_parquet(output_path, index=False)

    return output_path
