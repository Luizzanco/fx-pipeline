import pandas as pd
from pipeline.load import load_to_gold

def test_load_to_gold(tmp_path):
    # Cria arquivo Parquet fake
    silver_file = tmp_path / "test.parquet"
    df = pd.DataFrame({"currency": ["EUR", "BRL"], "rate": [0.9, 5.0]})
    df.to_parquet(silver_file)

    # Arquivo de saída
    gold_file = tmp_path / "fx_rates_latest.parquet"

    # Executa load
    result = load_to_gold(str(silver_file), str(gold_file))

    # Verifica se retornou caminho correto
    assert result == str(gold_file)
    # Verifica se o arquivo existe e contém dados
    df_gold = pd.read_parquet(result)
    assert not df_gold.empty
    assert "currency" in df_gold.columns
