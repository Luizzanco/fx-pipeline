import requests
import json
from pathlib import Path
from .utils import setup_logger, filename_for_date, now_iso

logger = setup_logger("ingest")

def ingest_rates(cfg):
    base_url = cfg["api"]["base_url"].rstrip("/")
    api_key = cfg["api"]["key"]
    base = cfg["api"].get("default_base", "USD")

    # Many exchangerate-api endpoints use /v6/YOUR-KEY/latest/USD pattern (adjust if different)
    url = f"{base_url}/{api_key}/latest/{base}"
    logger.info(f"Fetching rates from {url}")
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    # attach ingest timestamp
    data["_ingested_at"] = now_iso()

    raw_dir = Path(cfg["paths"]["raw"])
    filename = filename_for_date(prefix="", ext="json")
    target = raw_dir / filename
    logger.info(f"Saving raw JSON to {target}")
    with open(target, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return target
