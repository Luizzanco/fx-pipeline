import os
import json
import pandas as pd
from pathlib import Path

RAW_DIR = Path("data/raw")
SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")


def latest_file(path: Path, ext: str):
    """Retorna o arquivo mais recente em um diretório com a extensão desejada"""
    files = sorted(path.glob(f"*.{ext}"))
    return files[-1] if files else None


def show_raw():
    raw_file = latest_file(RAW_DIR, "json")
    if not raw_file:
        print("⚠️ Nenhum arquivo raw encontrado")
        return
    print(f"\n=== RAW === {raw_file}")
    with open(raw_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Mostra só alguns campos
    print("Base:", data.get("base_code") or data.get("base"))
    print("Data disponível:", list(data.keys())[:10])  # primeiras chaves
    print("Moedas disponíveis:", list(data.get("conversion_rates", {}).keys())[:10], "...")


def show_silver():
    silver_file = latest_file(SILVER_DIR, "parquet")
    if not silver_file:
        print("⚠️ Nenhum arquivo silver encontrado")
        return
    print(f"\n=== SILVER === {silver_file}")
    df = pd.read_parquet(silver_file)
    print(df.head())
    print(f"Linhas totais: {len(df)}")


def show_gold():
    gold_file = latest_file(GOLD_DIR, "parquet")
    if not gold_file:
        print("⚠️ Nenhum arquivo gold encontrado")
        return
    print(f"\n=== GOLD === {gold_file}")
    df = pd.read_parquet(gold_file)
    print(df.head())
    print(f"Linhas totais: {len(df)}")


if __name__ == "__main__":
    show_raw()
    show_silver()
    show_gold()
