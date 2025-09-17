import os
import pandas as pd


def load_to_gold(input_path: str, output_path: str = "data/gold/fx_rates_latest.parquet") -> str:
    """
    Lê o arquivo parquet (silver) e salva no gold.

    Args:
        input_path (str): Caminho do arquivo Parquet da camada silver.
        output_path (str): Caminho onde o arquivo Parquet será salvo (gold).

    Returns:
        str: Caminho final do arquivo salvo.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df = pd.read_parquet(input_path)   # lê do silver
    df.to_parquet(output_path, index=False)

    return output_path
