import pandas as pd
from pipeline.transform import transform_raw_to_silver

def test_transform_raw_to_silver(tmp_path):
    # Cria arquivo JSON fake de entrada
    raw_file = tmp_path / "test.json"
    raw_file.write_text('{"base_code": "USD", "conversion_rates": {"EUR": 0.9, "BRL": 5.0}}')

    # Arquivo de saída
    silver_file = tmp_path / "test.parquet"

    # Executa transformação
    result = transform_raw_to_silver(str(raw_file), str(silver_file))

    # Verifica se retornou caminho correto
    assert result == str(silver_file)
    # Verifica se o Parquet existe
    df = pd.read_parquet(result)
    assert "currency" in df.columns
    assert "rate" in df.columns
    assert not df.empty
