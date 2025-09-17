import os
import json
import requests
from datetime import datetime
from pathlib import Path


def ingest_rates(base_currency: str, output_dir: str) -> str:
    """
    Faz a ingestão das taxas de câmbio e salva em JSON (camada raw).

    Args:
        base_currency (str): Moeda base (ex: "USD").
        output_dir (str): Diretório onde salvar o arquivo JSON.

    Returns:
        str: Caminho final do arquivo salvo.
    """
    os.makedirs(output_dir, exist_ok=True)

    url = f"https://open.er-api.com/v6/latest/{base_currency}"
    resp = requests.get(url)
    data = resp.json()
    data["_ingested_at"] = datetime.utcnow().isoformat()

    file_path = Path(output_dir) / f"fx_rates_{base_currency}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return str(file_path)
