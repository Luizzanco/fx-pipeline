import os
from pathlib import Path
import pandas as pd
from .logger import log

def load_rates(df: pd.DataFrame, gold_path="data/gold/fx_rates_latest.parquet") -> str:
    """
    Salva o DataFrame final em parquet na pasta gold.
    Retorna o caminho do arquivo salvo.
    """
    gold_dir = Path(gold_path).parent
    os.makedirs(gold_dir, exist_ok=True)
    df.to_parquet(gold_path, index=False)
    log.info(f"Saved gold parquet to {gold_path}")
    return gold_path
